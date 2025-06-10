import json
import logging
import requests
import time
import boto3
import os
from bs4 import BeautifulSoup as bs
# from selenium_stealth import stealth
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict
import psycopg2
from datetime import datetime
from stealthenium import stealth
from abc import ABC, abstractmethod
from contextlib import contextmanager

QUEUE_URL = os.environ["QUEUE_URL"]
REGION = os.environ.get("REGION", "ap-northeast-2")
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = os.environ["DB_PORT"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]
SNS_ARN = os.environ["SNS_ARN"]
TREND_SNS_ARN = os.environ["TREND_SNS_ARN"]

ARCA_LIVE_LINK = "https://arca.live/b/hotdeal"
RULI_WEB_LINK = "https://bbs.ruliweb.com/market/board/1020?view=default"
PPOM_PPU_LINK = "https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu"
QUASAR_ZONE_LINK = "https://quasarzone.com/bbs/qb_saleinfo"
FM_KOREA_LINK = "https://www.fmkorea.com/hotdeal"

session = requests.Session()
retry = Retry(connect=2, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

item_link_dict = defaultdict(list)
db_config = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": DB_PORT
    }


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

class PAGES(ABC): 
    """각 Page들의 SuperClass"""
    def __init__(self, driver):
        self.item_link_list = []
        self.trend_item_link_list = []
        self.get_item_driver = driver
        
    def pub_item_links(self):
        """SNS로 Scan 정보 Publish"""
        sns = boto3.client('sns', region_name=REGION)
        topic_arn = SNS_ARN
        db_item_links = self.db_get_item_links()
        _item_link_list = list(set(self.item_link_list) - set(db_item_links))
        print(f"new item links : {_item_link_list}")
        if _item_link_list:
            message_body = json.dumps(_item_link_list)
            scanned_site = self.__class__.__name__
            num_item_links = str(len(_item_link_list))
            
            response = sns.publish(
                TopicArn=topic_arn,
                Message=message_body,
                MessageAttributes = {
                    "is_scanning" : {'DataType': 'String', 'StringValue': "1"},
                    "scanned_site" : {'DataType': 'String', 'StringValue': scanned_site},
                    "num_item_links" : {'DataType': 'String', 'StringValue': num_item_links}
                    
                }
            )
            print(response)
        else:
            print("not found new item links")

    def pub_trend_item_links(self):
        """SNS로 인기글 정보 Publish"""
        sns = boto3.client('sns', region_name=REGION)
        topic_arn = TREND_SNS_ARN
        db_trend_item_links = self.db_get_trend_item_links()
        _trend_item_link_list = list(set(self.trend_item_link_list) - set(db_trend_item_links))
        print(f"new trend item links : {_trend_item_link_list}")
        if _trend_item_link_list:
            message_body = json.dumps(_trend_item_link_list)
            scanned_site = self.__class__.__name__
            num_item_links = str(len(_trend_item_link_list))
            
            response = sns.publish(
                TopicArn=topic_arn,
                Message=message_body,
                MessageAttributes = {
                    "is_scanning" : {'DataType': 'String', 'StringValue': "1"},
                    "scanned_site" : {'DataType': 'String', 'StringValue': scanned_site},
                    "num_item_links" : {'DataType': 'String', 'StringValue': num_item_links}
                    
                }
            )
            print(response)
        else:
            print("not found new trend item links")
            
    def db_get_item_links(self):
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            table_name = self.__class__.__name__.lower()
            cursor.execute(
                f"""
                SELECT *
                FROM {table_name}_item_links
                WHERE id > (SELECT MAX(id) - 100 FROM {table_name}_item_links);
                """
            )
            rows = cursor.fetchall()
            db_item_links = [i[0] for i in rows]
            return db_item_links
        
        except Exception as e:
            print(str(e))
        
    def db_get_trend_item_links(self):
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            table_name = self.__class__.__name__.lower()
            cursor.execute(
                f"""
                SELECT *
                FROM {table_name}_trend_item_links
                WHERE id > (SELECT MAX(id) - 100 FROM {table_name}_trend_item_links);
                """
            )
            rows = cursor.fetchall()
            db_item_links = [i[0] for i in rows]
            return db_item_links
        
        except Exception as e:
            print(str(e))
    
    @abstractmethod
    def get_item_links(self):
        pass
        
    @abstractmethod
    def get_trend_item_links(self):
        pass
        
    def scanning(self):
        with timer(f"{self.__class__.__name__} get item link"):
            self.get_item_links()
        with timer(f"{self.__class__.__name__} get trend item link"):
            self.get_trend_item_links()
        
        try:                
            self.pub_item_links()
            self.pub_trend_item_links()
        except Exception as e:
            print(f"fail pub item links {e}")
            
class QUASAR_ZONE(PAGES):
    def __init__(self, driver):
        self.site_name = QUASAR_ZONE_LINK
        super().__init__(driver)
        
    def is_trend_item(self, item):
        try:
            comment_count = 0
            comment_count = item.find_element(By.CLASS_NAME, "board-list-comment")
            comment_count = int(comment_count.text)
            if comment_count >= 10:
                return True
        
        except Exception as e:
            return False
        
        finally:
            return False
        
    def get_trend_item_links(self):
        self.get_item_driver.implicitly_wait(1)
        for i in range(1, 31):
            try:
                find_css_selector = f"#frmSearch > div > div.list-board-wrap > div.market-type-list.market-info-type-list.relative > table > tbody > tr:nth-child({i}) > td:nth-child(2) > div > div.market-info-list-cont > p > a"
                trend_item_link = "err"
                item = self.get_item_driver.find_element(By.CSS_SELECTOR, find_css_selector)       
                trend_item_link = item.get_attribute("href")
 
                comment_count = item.find_element(By.CLASS_NAME, "board-list-comment")
                comment_count = int(comment_count.text)
                if comment_count >= 10:
                    self.trend_item_link_list.append(trend_item_link)
                    
                print(f"{trend_item_link} num comment : {comment_count}")
         
            except Exception as e:
                print(f"no comment {trend_item_link}")
                
        self.get_item_driver.implicitly_wait(10)    
        
    # def get_item_links(self):
    #     try:
    #         self.get_item_driver.get(self.site_name)
    #     except Exception as e:
    #         print(f"{self.site_name} 접속 실패 {str(e)}")
    #         return
        
    #     for i in range(1, 31):
    #         try:
    #             find_css_selector = f"#frmSearch > div > div.list-board-wrap > div.market-type-list.market-info-type-list.relative > table > tbody > tr:nth-child({i}) > td:nth-child(2) > div > div.market-info-list-cont > p > a"
    #             item_link = "err"
    #             item = self.get_item_driver.find_element(By.CSS_SELECTOR, find_css_selector)
    #             print(item.text)
    #             item_link = item.get_attribute("href")
    #             self.item_link_list.append(item_link)
    #             print(item_link)
                    
    #         except Exception as e:
    #             print(f"fail get item links {item_link} {e}")
    #             capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
    #             break
        
    def get_item_links(self):
        try:
            self.get_item_driver.get(self.site_name)
        except Exception as e:
            print(f"{self.site_name} 접속 실패 {str(e)}")
            return
        
        for i in range(1, 31):
            try:
                item_link = "err"
                find_item_css_selector = f"#frmSearch > div > div.list-board-wrap > div.market-type-list.market-info-type-list.relative > table > tbody > tr:nth-child({i}) > td:nth-child(2) > div"
                find_item_link_css_selector = " div.market-info-list-cont > p > a"
                item = self.get_item_driver.find_element(By.CSS_SELECTOR, find_item_css_selector)
                item_link = item.find_element(By.CSS_SELECTOR, find_item_link_css_selector).get_attribute("href")
                self.item_link_list.append(item_link)
                print(item_link)
                    
                if self.is_trend_item(item):
                    self.trend_item_link_list.append(item_link)
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break

class ARCA_LIVE(PAGES):
    def __init__(self, driver):
        self.site_name = ARCA_LIVE_LINK
        super().__init__(driver)
    
    def get_trend_item_links(self):
        self.get_item_driver.implicitly_wait(1)    
        for i in range(2, 27):
            try:
                find_link_xpath_selector = f"/html/body/div[2]/div[3]/article/div/div[6]/div[2]/div[{i}]/div/div/span[2]/a"
                find_comment_count_xpath_selector = f"/html/body/div[2]/div[3]/article/div/div[6]/div[2]/div[{i}]/div/div/span[2]/a/span[2]/span"
                trend_item_link = "err"
                item = self.get_item_driver.find_element(By.XPATH, find_link_xpath_selector)
                trend_item_link = item.get_attribute("href")
                
                comment_count = self.get_item_driver.find_element(By.XPATH, find_comment_count_xpath_selector)
                comment_count = int(comment_count.text[1:-1])
                if comment_count >= 10:
                    self.trend_item_link_list.append(trend_item_link)
                    
                print(f"{trend_item_link} num comment : {comment_count}")
                
            except Exception as e:
                print(f"no comment {trend_item_link}")
                
        self.get_item_driver.implicitly_wait(10)
        
    def get_item_links(self):
        try:
            self.get_item_driver.get(self.site_name)
        except Exception as e:
            print(f"{self.site_name} 접속 실패 {str(e)}")
            return
        
        for i in range(2, 27):
            try:
                find_xpath_selector = f"/html/body/div[2]/div[3]/article/div/div[6]/div[2]/div[{i}]/div/div/span[2]/a"
                item_link = "err"
                item = self.get_item_driver.find_element(By.XPATH, find_xpath_selector)
                print(item.text)
                item_link = item.get_attribute("href")
                self.item_link_list.append(item_link)
                print(item_link)
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break
            
# class RULI_WEB(PAGES):
#     def __init__(self):
#         self.site_name = RULI_WEB_LINK
#         super().__init__()
        
#     def get_item_links(self, driver):
#         get_item_driver = driver
#         try:
#             get_item_driver.get(self.site_name)
#         except Exception as e:
#             print(f"{self.site_name} 접속 실패 {str(e)}")
#             return
        
#         for i in range(8, 36):
#             try:
#                 find_css_selector = f"#board_list > div > div.board_main.theme_default.theme_white.theme_white > table > tbody > tr:nth-child({i}) > td.subject > div > a.deco"
#                 item_link = "err"
#                 item = get_item_driver.find_element(By.CSS_SELECTOR, find_css_selector)
#                 item_link = item.get_attribute("href")
#                 self.item_link_list.append(item_link)
#                 print(item_link)
#             except Exception as e:
#                 print(f"fail get item links {item_link} {e}")
#                 capture_and_send_screenshot(get_item_driver, self.__class__.__name__)
#                 break

#         try:                
#             self.pub_item_links()
#         except Exception as e:
#             print(f"fail pub item links {e}")
            
class FM_KOREA(PAGES):
    def __init__(self, driver):
        self.site_name = FM_KOREA_LINK
        super().__init__(driver)
        
    def get_trend_item_links(self):
        self.get_item_driver.implicitly_wait(1)
        for i in range(1, 21):
            try:
                find_css_selector = f"#bd_1196365581_0 > div > div.fm_best_widget._bd_pc > ul > li:nth-child({i}) > div > h3 > a"
                trend_item_link = "err"
                item = self.get_item_driver.find_element(By.CSS_SELECTOR, find_css_selector)
                trend_item_link = item.get_attribute("href")
                
                comment_count = item.find_element(By.CLASS_NAME, "comment_count")
                comment_count = int(comment_count.text[1:-1])
                
                if comment_count >= 10:
                    self.trend_item_link_list.append(trend_item_link)
                    
                print(f"{trend_item_link} num comment : {comment_count}")
            except Exception as e:
                print(f"no comment {trend_item_link}")

        self.get_item_driver.implicitly_wait(10)
        
    def get_item_links(self):
        try:
            self.get_item_driver.get(self.site_name)
        except Exception as e:
            print(f"{self.site_name} 접속 실패 {str(e)}")
            return

        for i in range(1, 21):
            try:
                find_css_selector = f"#bd_1196365581_0 > div > div.fm_best_widget._bd_pc > ul > li:nth-child({i}) > div > h3 > a"
                item_link = "err"
                item = self.get_item_driver.find_element(By.CSS_SELECTOR, find_css_selector)
                print(item.text)
                item_link = item.get_attribute("href")
                self.item_link_list.append(item_link)
                print(item_link)

                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break
        
class PPOM_PPU(PAGES):
    def __init__(self, driver):
        self.site_name = PPOM_PPU_LINK
        super().__init__(driver)
    
    def get_trend_item_links(self):
        response = session.get(self.site_name)
        soup = bs(response.content, "html.parser")
        for item in soup.find_all(class_= "baseList-c")[1:20]: # 댓글이 달린 게시글만
            trend_item_link = "err"
            if "popup_comment.php" not in item.get("onclick", ""): # 공지, 광고 제외
                trend_item_link = "https://www.ppomppu.co.kr/zboard/view.php" + item.attrs["onclick"][13:-3]
                if int(item.text) >= 10: # 댓글 10개 이상
                    self.trend_item_link_list.append(trend_item_link)
                try:
                    print(f"{trend_item_link} num comment : {item.text}")
                except:
                    print(f"no comment {trend_item_link}")
                    
    def get_item_links(self):
        response = session.get(self.site_name)
        soup = bs(response.content, "html.parser")
        for item in soup.find_all(class_= "baseList-thumb")[:20]:
            try:
                item_link = "https://www.ppomppu.co.kr/zboard/" + item.attrs["href"]
                item_link = item_link.replace("&&", "&")
                self.item_link_list.append(item_link)
                print(item_link)
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                break
        

    def scanning(self):
        self.get_item_links()
        self.get_trend_item_links()
        
        try:                
            self.pub_item_links()
            self.pub_trend_item_links()
        except Exception as e:
            print(f"fail pub item links {e}")
        
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

@contextmanager
def timer(name: str):
    t0 = time.time()
    try:
        yield
    finally:
        print(f"{name} done in {time.time() - t0:.3f} s")


def handler(event=None, context=None):
    
    driver = set_driver()
    quasar_zone = QUASAR_ZONE(driver)
    ppom_ppu = PPOM_PPU(driver)
    fm_korea = FM_KOREA(driver)
    # ruli_web = RULI_WEB(driver)
    arca_live = ARCA_LIVE(driver)
    
    # 루리웹 접속 불가로 인해 주석 처리 Message: unknown error: net::ERR_CONNECTION_TIMED_OUT
    # current = time.time()
    # ruli_web.get_item_links(driver)
    # print(f" ruliweb {time.time() - current}")
    
    with timer("quasar zone"):
        quasar_zone.get_item_links()
    
    with timer("ppom ppu"):
        ppom_ppu.scanning()
    
    with timer("fm korea"):
        fm_korea.scanning()
    
    with timer("arca live"):
        arca_live.scanning()

    driver.quit()
    
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "scanning success",
            }
        ),
    }

if __name__ == '__main__':
    handler()
