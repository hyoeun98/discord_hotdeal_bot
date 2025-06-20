import json
import logging
import requests
import time
import re
import boto3
import ast
import os
from bs4 import BeautifulSoup as bs
from selenium_stealth import stealth
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

QUEUE_URL = os.environ["QUEUE_URL"]
REGION = os.environ.get("REGION", "ap-northeast-2")
DISCORD_WEBHOOK = os.environ["DISCORD"]

ARCA_LIVE_LINK = "https://arca.live/b/hotdeal"
RULI_WEB_LINK = "https://bbs.ruliweb.com/market/board/1020?view=default"
PPOM_PPU_LINK = "https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu"
QUASAR_ZONE_LINK = "https://quasarzone.com/bbs/qb_saleinfo"
FM_KOREA_LINK = "https://www.fmkorea.com/hotdeal"
COOL_ENJOY_LINK = "https://coolenjoy.net/bbs/jirum"
EOMI_SAE_LINK = "https://eomisae.co.kr/fs"

session = requests.Session()
retry = Retry(connect=2, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

def capture_and_send_screenshot(driver, file_name):
    """화면 캡처 후 Discord로 직접 전송"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_file = f"/tmp/{file_name}_{timestamp}.png"
    
    # 화면 캡처
    driver.save_screenshot(local_file)
    
    # Discord로 전송
    with open(local_file, "rb") as f:
        files = {"file": (local_file, f, "image/png")}
        payload = {"content": f"에러 발생 스크린샷"}
        response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)
    
    page_source = driver.page_source

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = f'/tmp/{timestamp}_page.html'
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(page_source)
    
    with open(file_path, "rb") as f:
        files = {"file": (file_path, f, "text/html")}
        payload = {"content": f"에러 발생 페이지 소스"}
        response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)
    
    # 전송 결과 확인
    print(f"response code : {response.status_code}")
    print(response.text)
    
class PAGES: 
    """각 Page들의 SuperClass"""
    def __init__(self):
        self.refresh_delay = 30 # sec
        
    def pub_item_links(self, message):
        """SQS로 Crawl 정보 Publish"""
        sqs = boto3.client('sqs', region_name=REGION)
        queue_url = QUEUE_URL
        message_body = json.dumps(message)
        crawled_site = self.__class__.__name__
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageAttributes = {
                "is_crawling" : {'DataType': 'String', 'StringValue': "1"},
                "site_name" : {'DataType': 'String', 'StringValue': crawled_site},
                
            }
        )
        
class QUASAR_ZONE(PAGES):
    def __init__(self):
        self.site_name = QUASAR_ZONE_LINK

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment, category = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name = driver.find_element(By.CSS_SELECTOR, "#content > div > div.sub-content-wrap > div.left-con-wrap > div.common-view-wrap.market-info-view-wrap > div > dl > dt > div:nth-child(1) > h1").text.split()[2:]
                item_name = " ".join(item_name)
                created_at = driver.find_element(By.CSS_SELECTOR, "#content > div > div.sub-content-wrap > div.left-con-wrap > div.common-view-wrap.market-info-view-wrap > div > dl > dt > div.util-area > p > span").text
                content = driver.find_element(By.CSS_SELECTOR, "#new_contents").text
                comment = list(map(lambda x: x.text, driver.find_elements(By.CSS_SELECTOR, "#content > div.sub-content-wrap > div.left-con-wrap > div.reply-wrap > div.reply-area > div.reply-list")))
                category = driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div/div[1]/div[1]/div[4]/div/dl/dt/div[3]/div/div[1]").text
                shopping_mall_link = driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div/div[1]/div[1]/div[4]/div/dl/dd/table/tbody/tr[1]/td/a").text
                shopping_mall = driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div/div[1]/div[1]/div[4]/div/dl/dd/table/tbody/tr[2]/td").text
                price = driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div/div[1]/div[1]/div[4]/div/dl/dd/table/tbody/tr[3]/td").text
                delivery = driver.find_element(By.XPATH, "/html/body/div[3]/div/div/div/div[1]/div[1]/div[4]/div/dl/dd/table/tbody/tr[4]/td").text
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                
            finally:
                result = {
                    "created_at" : created_at,
                    "item_link" : item_link,
                    "shopping_mall_link" : shopping_mall_link,
                    "shopping_mall" : shopping_mall,
                    "price" : price,
                    "item_name" : item_name,
                    "delivery" : delivery,
                    "content" : content,
                    "category" : category
                }
                print(result)
                self.pub_item_links(result)


class ARCA_LIVE(PAGES):
    def __init__(self):
        self.site_name = ARCA_LIVE_LINK
        
    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment, category = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                table = driver.find_element(By.TAG_NAME, "table")
                rows = table.find_elements(By.TAG_NAME, "tr")
                details = [row.text for row in rows]
                shopping_mall_link, shopping_mall, item_name, price, delivery = list(map(lambda x: "".join(x.split()[1:]), details))
                content = driver.find_element(By.CSS_SELECTOR, "body > div.root-container > div.content-wrapper.clearfix > article > div > div.article-wrapper > div.article-body > div.fr-view.article-content").text
                comment_box = driver.find_element(By.CSS_SELECTOR, "#comment > div.list-area")
                comment = list(map(lambda x: x.text, comment_box.find_elements(By.CLASS_NAME, "text")))
                created_at = driver.find_element(By.CSS_SELECTOR, "body > div.root-container > div.content-wrapper.clearfix > article > div > div.article-wrapper > div.article-head > div.info-row > div.article-info.article-info-section > span:nth-child(12) > span.body > time").text
                category = driver.find_element(By.XPATH, "/html/body/div[2]/div[3]/article/div/div[2]/div[2]/div[1]/div[2]/span").text
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                
            finally:
                result = {
                    "created_at" : created_at,
                    "item_link" : item_link,
                    "shopping_mall_link" : shopping_mall_link,
                    "shopping_mall" : shopping_mall,
                    "price" : price,
                    "item_name" : item_name,
                    "delivery" : delivery,
                    "content" : content,
                    "category" : category
                }
                print(result)
                self.pub_item_links(result)

class RULI_WEB(PAGES):
    def __init__(self):
        self.site_name = RULI_WEB_LINK

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment, category = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_top > div.user_view > div:nth-child(1) > div > div > h4 > span > span.subject_inner_text").text
                shopping_mall = re.findall(r"\[.+\]", item_name)[0]
                created_at = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_top > div.user_view > div.row.user_view_target > div.col.user_info_wrapper > div > p:nth-child(6) > span").text
                content = driver.find_element(By.TAG_NAME, "article").text
                comment = list(map(lambda x: x.text, driver.find_elements(By.CLASS_NAME, "comment")))
                shopping_mall_link = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_view > div.row.relative > div > div.source_url.box_line_with_shadow > a").text
                category = driver.find_element(By.XPATH, "/html/body/div[4]/div[2]/div[2]/div/div/div[2]/div/div[2]/div[1]/div[1]/div[1]/div/div/h4/span/span[1]").text

            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                
            finally:
                result = {
                    "created_at": created_at,
                    "item_link": item_link,
                    "shopping_mall_link": shopping_mall_link,
                    "shopping_mall": shopping_mall,
                    "price": price,
                    "item_name": item_name,
                    "delivery": delivery,
                    "content": content,
                    "category": category,
                }
                print(result)
                self.pub_item_links(result)
                
class FM_KOREA(PAGES):
    def __init__(self):
        self.site_name = FM_KOREA_LINK
    
    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment, category = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                details = driver.find_elements(By.CLASS_NAME, "xe_content")
                shopping_mall_link, shopping_mall, item_name, price, delivery, content, *comment = details
                shopping_mall_link, shopping_mall, item_name, price, delivery, content = map(lambda x: x.text, (shopping_mall_link, shopping_mall, item_name, price, delivery, content))
                comment = list(map(lambda x: x.text, comment))
                created_at = driver.find_element(By.CSS_SELECTOR, "#bd_capture > div.rd_hd.clear > div.board.clear > div.top_area.ngeb > span").text
                category = driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div/div[4]/div/div[2]/div[2]/div/div[1]/span/a").text
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                
            finally:
                result = {
                    "created_at" : created_at,
                    "item_link" : item_link,
                    "shopping_mall_link" : shopping_mall_link,
                    "shopping_mall" : shopping_mall,
                    "price" : price,
                    "item_name" : item_name,
                    "delivery" : delivery,
                    "content" : content,
                    "category" : category
                }
                print(result)
                self.pub_item_links(result)
                
                
class PPOM_PPU(PAGES):
    def __init__(self):
        self.site_name = PPOM_PPU_LINK
                
    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
    
            try:
                created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment, category = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name = driver.find_element(By.CSS_SELECTOR, "#topTitle > h1").text
                content = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[3]/div/table[3]/tbody/tr[1]/td/table/tbody/tr/td").text
                comment = driver.find_element(By.ID, "quote").text
                created_at = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[3]/div/div[3]/div/ul/li[2]").text.lstrip("등록일 ")
                shopping_mall_link = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[3]/div/div[3]/div/ul/li[4]/a").text
                shopping_mall = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[3]/div/div[3]/h1/span").text
                
            except Exception as e:
                # major한 shopping_mall이 아니면 path가 달라짐
                if item_name != "err" and shopping_mall == "err":
                     shopping_mall = re.match("\[.+\]", item_name)[0]
                print(f"fail get item link {item_link} {str(e)}")
                
            finally:
                result = {
                    "created_at" : created_at,
                    "item_link" : item_link,
                    "shopping_mall_link" : shopping_mall_link,
                    "shopping_mall" : shopping_mall,
                    "price" : price,
                    "item_name" : item_name,
                    "delivery" : delivery,
                    "content" : content,
                    "category" : category
                }
                print(result)
                self.pub_item_links(result)

class COOL_ENJOY(PAGES):
    def __init__(self):
        self.site_name = COOL_ENJOY_LINK
                
    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
    
    
            try:
                created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment, category = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                title_text = driver.find_element(By.XPATH, '//*[@id="bo_v_title"]').text
                split_idx = title_text.find("|")
                category = title_text[:split_idx-3].strip()
                item_name = title_text[split_idx+1:].strip()
                content = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[1]/div[2]/article/section[2]/div/div[2]").text
                comment = driver.find_element(By.ID, "bo_vc").text
                created_at = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[2]/div[1]/div[2]/article/section[1]/div[1]/ul/li[3]/time").text
                shopping_mall_link = driver.find_element(By.XPATH, '//*[@id="bo_v_atc"]/ul/li/div/div/div[2]/a').text
                shopping_mall_candidate = re.search(r'\[([^]]+)\]', item_name)
                if shopping_mall_candidate:
                    shopping_mall = shopping_mall_candidate.group(1)
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                
            finally:
                result = {
                    "created_at" : created_at,
                    "item_link" : item_link,
                    "shopping_mall_link" : shopping_mall_link,
                    "shopping_mall" : shopping_mall,
                    "price" : price,
                    "item_name" : item_name,
                    "delivery" : delivery,
                    "content" : content,
                    "category" : category
                }
                print(result)
                self.pub_item_links(result)
                           
class EOMI_SAE(PAGES):
    def __init__(self):
        self.site_name = EOMI_SAE_LINK
                
    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
    
    
            try:
                created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment, category = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name = driver.find_element(By.XPATH, '//*[@id="D_"]/div[1]/div[1]/div[1]/h2/a').text
                category = driver.find_element(By.XPATH, '//*[@id="D_"]/div[1]/div[1]/div[1]/h2/span').text
                content = driver.find_element(By.XPATH, '//*[@id="D_"]/div[1]/div[2]/article/div[3]').text
                comment = driver.find_element(By.CLASS_NAME, "_comment").text
                created_at = driver.find_element(By.XPATH, '//*[@id="D_"]/div[1]/div[1]/div[2]/span[5]').text
                shopping_mall_link = driver.find_element(By.XPATH, '//*[@id="D_"]/div[1]/div[2]/table/tbody/tr/td/a').get_attribute("href")
                
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                
            finally:
                result = {
                    "created_at" : created_at,
                    "item_link" : item_link,
                    "shopping_mall_link" : shopping_mall_link,
                    "shopping_mall" : shopping_mall,
                    "price" : price,
                    "item_name" : item_name,
                    "delivery" : delivery,
                    "content" : content,
                    "category" : category
                }
                print(result)
                self.pub_item_links(result)
                     
def set_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = "/opt/chrome/chrome"
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_options.add_argument('window-size=1392x1150')
    chrome_options.add_argument("disable-gpu")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument('--incognito')
    service = Service(executable_path="/opt/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    driver.implicitly_wait(10)
    return driver
    
    
def handler(event, context):
    try:
        driver = set_driver()
        
        message = event['Records'][0]['Sns']['MessageAttributes']
        site_name = message['site_name']['Value']
        item_link_list = ast.literal_eval(event['Records'][0]['Sns']['Message'])
        
        # 크롤링 클래스 선택
        crawler_class = globals()[site_name]
        crawler = crawler_class()
        
        crawler.crawling(driver, item_link_list)
            
        driver.quit()
        return {
            'statusCode': 200,
            'body': json.dumps('Crawling completed successfully')
        }
        
    except Exception as e:
        print(f"Error in handler: {str(e)}")
        if 'driver' in locals():
            driver.quit()
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error during crawling: {str(e)}')
        }

if __name__ == '__main__':
    handler(event, context)
  
