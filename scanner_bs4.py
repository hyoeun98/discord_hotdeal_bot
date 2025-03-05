
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import Keys, ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.options import Options
from kafka import KafkaConsumer, KafkaProducer
from collections import deque
import json
from enum import Enum
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from selenium.webdriver.remote.errorhandler import WebDriverException
import requests
from bs4 import BeautifulSoup
from selenium_stealth import stealth
import logging
import re
import random
import time
import concurrent.futures
from datetime import datetime
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import base64
import requests

load_dotenv()


ARCA_LIVE_LINK = "https://arca.live/b/hotdeal"
RULI_WEB_LINK = "https://bbs.ruliweb.com/market/board/1020?view=default"
PPOM_PPU_LINK = "https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu"
QUASAR_ZONE_LINK = "https://quasarzone.com/bbs/qb_saleinfo"
FM_KOREA_LINK = "https://www.fmkorea.com/hotdeal"


DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")

def set_driver():
    chrome_options = Options()
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_options.add_argument('--block-new-web-contents')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    # chrome_options.add_argument('--window-size=1920x1080')
    chrome_options.add_argument("--disable-extensions")
    driver = webdriver.Chrome(options = chrome_options)
    stealth(driver,
        languages=['en-US','en'],
        vendor='Google Inc.',
        platform='Win32',
        webgl_vendor='Intel Inc.',
        renderer='Intel Iris OpenGL Engine',
        fix_hairline=True)
    driver.implicitly_wait(10)
    return driver

def save_full_screenshot(driver, screenshot_filename):
    try:
        page_rect = driver.execute_cdp_cmd('Page.getLayoutMetrics', {})
        screenshot_config = {'captureBeyondViewport': True,
                                'fromSurface': True,
                                'clip': {'width': page_rect['cssContentSize']['width'],
                                        'height': page_rect['cssContentSize']['height'], #contentSize -> cssContentSize
                                        'x': 0,
                                        'y': 0,
                                        'scale': 1},
                                }
        base_64_png = driver.execute_cdp_cmd('Page.captureScreenshot', screenshot_config)
        with open(screenshot_filename, "wb") as fh:
            fh.write(base64.urlsafe_b64decode(base_64_png['data']))
    except Exception as e:
        logging.info(f"screenshot fail {screenshot_filename}")
        # driver.save_screenshot(filename)
        
def error_logging(class_name, driver, e: Exception, error_type, item_link, **kwargs):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # 현재 시간을 포맷팅
    error_log = {"error_log": e, "time": timestamp, "error_type": error_type}
    screenshot_filename = f'error_screenshot/{class_name}_{timestamp}.png'
    
    if kwargs:
        for k, v in kwargs:
            error_log[k] = v
    logging.info(error_log)
    
    try:
        save_full_screenshot(driver, screenshot_filename)
        with open(screenshot_filename, 'rb') as file:
            response = client.files_upload_v2(
                channel=SLACK_CHANNEL_ID,
                file=file,
                filename=os.path.basename(screenshot_filename),  # 파일 이름
                initial_comment=error_log  # 업로드할 때 이미지 제목
            )
        logging.info(f"File uploaded successfully: {response['file']['permalink']}")
    except SlackApiError as e:
        logging.info(f"Error uploading file: {e.response['error']}")
    
    try:
        cursor.execute(error_insert_query, (class_name, str(error_log), timestamp, item_link))
        logging.info(f"Table error insert complete")
        connection.commit()
    except Exception as e:
        logging.info(e)
        logging.info(f"Table error insert fail")
        connection.rollback()
        
class PathFinder:
    def __init__(self):
        self.driver = set_driver()
    
class PAGES:
    def __init__(self, pathfinder):
        self.refresh_delay = 30 # sec
        self.driver = pathfinder.driver
            
    def pub_hot_deal_page(self, item_link): # crawling 할 page를 publish
        try:
            cursor.execute("SELECT EXISTS(SELECT 1 FROM pages WHERE item_link = %s)", (item_link,))
            exists = cursor.fetchone()[0]
        except Exception as e:
            logging.info(e)
            
        if not exists:
            try:
                logging.info((self.__class__.__name__, item_link))
                cursor.execute(page_insert_query, (self.__class__.__name__, item_link))
                connection.commit()
            except Exception as e:
                logging.info(e)
                connection.rollback()
            producer.send(topic = 'test', key = self.__class__.__name__, value=item_link)
            producer.flush()
            
class ARCA_LIVE(PAGES): # shopping_mall_link, shopping_mall, item_name, price, delivery, content, comment
    def __init__(self, pathfinder):
        self.site_name = ARCA_LIVE_LINK
        super().__init__(pathfinder)
        
    def get_item_links(self):
        get_item_driver = self.driver
        get_item_driver.get(self.site_name)
        for i in range(2, 27):
            try:
                # "/html/body/div[2]/div[3]/article/div/div[6]/div[2]/div[45]"
                find_xpath_selector = f"/html/body/div[2]/div[3]/article/div/div[6]/div[2]/div[{i}]/div/a"
                item_link = "err"
                item = get_item_driver.find_element(By.XPATH, find_xpath_selector)
                item_link = item.get_attribute("href")
                self.pub_hot_deal_page(item_link)
            except Exception as e:
                error_logging(self.__class__.__name__, self.driver, e, f"fail get item links {item}", item_link)
    
    @staticmethod
    def crawling(driver, item_link):
        driver.get(item_link)
        try: # 신고 처리, 보안 검사 등
            created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment = "err", "err", "err", "err", "err", "err", "err", "err"
            table = driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.TAG_NAME, "tr")
            details = [row.text for row in rows]
            shopping_mall_link, shopping_mall, item_name, price, delivery = list(map(lambda x: "".join(x.split()[1:]), details))
            content = driver.find_element(By.CSS_SELECTOR, "body > div.root-container > div.content-wrapper.clearfix > article > div > div.article-wrapper > div.article-body > div.fr-view.article-content").text
            comment_box = driver.find_element(By.CSS_SELECTOR, "#comment > div.list-area")
            comment = list(map(lambda x: x.text, comment_box.find_elements(By.CLASS_NAME, "text")))
            created_at = driver.find_element(By.CSS_SELECTOR, "body > div.root-container > div.content-wrapper.clearfix > article > div > div.article-wrapper > div.article-head > div.info-row > div.article-info.article-info-section > span:nth-child(12) > span.body > time").text
        except Exception as e:
            error_logging("ARCA_LIVE", driver, e, f"crawling error, {item_link}", item_link)
            
        finally:
            result = {
                "created_at" : created_at,
                "item_link" : item_link,
                "shopping_mall_link" : shopping_mall_link,
                "shopping_mall" : shopping_mall,
                "price" : price,
                "item_name" : item_name,
                "delivery" : delivery,
                "content" : content
            }
            return result

# shopping_mall_link가 누락된 채로 게시글이 올라옴
class RULI_WEB(PAGES): # shopping_mall_link, item_name, content, comment
    def __init__(self, pathfinder):
        self.site_name = RULI_WEB_LINK
        super().__init__(pathfinder)
                
    def get_item_links(self):
        response = requests.get(self.site_name)
        soup = BeautifulSoup(response.content, "html.parser")
        for item in soup.find_all(class_= "table_body blocktarget"):
            try:
                item_link = item.find(class_ = "subject_link deco").attrs["href"]
                self.pub_hot_deal_page(item_link)
            except Exception as e:
                error_logging(self.__class__.__name__, self.driver, e, f"fail get item links {item}", item_link)
    
    @staticmethod
    def crawling(driver, item_link):
        driver.get(item_link)
        try: # 신고 처리, 보안 검사 등
            created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment = "err", "err", "err", "err", "err", "err", "err", "err"
            item_name = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_top > div.user_view > div:nth-child(1) > div > div > h4 > span > span.subject_inner_text").text
            shopping_mall = re.findall(r"\[.+\]", item_name)[0]
            created_at = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_top > div.user_view > div.row.user_view_target > div.col.user_info_wrapper > div > p:nth-child(6) > span").text
            content = driver.find_element(By.TAG_NAME, "article").text
            comment = list(map(lambda x: x.text, driver.find_elements(By.CLASS_NAME, "comment")))
            shopping_mall_link = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_view > div.row.relative > div > div.source_url.box_line_with_shadow > a").text
        except Exception as e:
            if not shopping_mall_link:
                pattern = r'https?://[^\s]+'
                links = re.findall(pattern, content)
                shopping_mall_link = links[-1]
            error_logging("RULI_WEB", driver, e, f"crawling error, {item_link}", item_link)
            
        finally:
            result = {
                "created_at" : created_at,
                "item_link" : item_link,
                "shopping_mall_link" : shopping_mall_link,
                "shopping_mall" : shopping_mall,
                "price" : price,
                "item_name" : item_name,
                "delivery" : delivery,
                "content" : content
            }
            return result
        
class FM_KOREA(PAGES): # shopping_mall_link, shopping_mall, item_name, price, delivery, content, comment
    def __init__(self, pathfinder):
        self.site_name = FM_KOREA_LINK
        super().__init__(pathfinder)
    
    def get_item_links(self):
        get_item_driver = self.driver
        get_item_driver.get(self.site_name)
        for i in range(1, 21):
            try:
                find_css_selector = f"#bd_1196365581_0 > div > div.fm_best_widget._bd_pc > ul > li:nth-child({i}) > div > h3 > a"
                item_link = "err"
                item = get_item_driver.find_element(By.CSS_SELECTOR, find_css_selector)
                item_link = item.get_attribute("href")
                self.pub_hot_deal_page(item_link)
            except Exception as e:
                error_logging(self.__class__.__name__, self.driver, e, f"fail get item links {find_css_selector}", item_link)
    
    @staticmethod
    def crawling(driver, item_link):
        driver.get(item_link)
        try: # 신고 처리, 보안 검사 등
            created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment = "err", "err", "err", "err", "err", "err", "err", "err"
            details = driver.find_elements(By.CLASS_NAME, "xe_content")
            shopping_mall_link, shopping_mall, item_name, price, delivery, content, *comment = details
            shopping_mall_link, shopping_mall, item_name, price, delivery, content = map(lambda x: x.text, (shopping_mall_link, shopping_mall, item_name, price, delivery, content))
            comment = list(map(lambda x: x.text, comment))
            created_at = driver.find_element(By.CSS_SELECTOR, "#bd_capture > div.rd_hd.clear > div.board.clear > div.top_area.ngeb > span").text
        except Exception as e:
            error_logging("FM_KOREA", driver, e, f"crawling error, {item_link}", item_link)
            
        finally:
            result = {
                "created_at" : created_at,
                "item_link" : item_link,
                "shopping_mall_link" : shopping_mall_link,
                "shopping_mall" : shopping_mall,
                "price" : price,
                "item_name" : item_name,
                "delivery" : delivery,
                "content" : content
            }
            return result
        
class QUASAR_ZONE(PAGES):
    def __init__(self, pathfinder):
        self.site_name = QUASAR_ZONE_LINK
        super().__init__(pathfinder)
        
    def get_item_links(self):
        get_item_driver = self.driver
        get_item_driver.get(self.site_name)
        for i in range(1, 31):
            try:
                find_css_selector = f"#frmSearch > div > div.list-board-wrap > div.market-type-list.market-info-type-list.relative > table > tbody > tr:nth-child({i}) > td:nth-child(2) > div > div.market-info-list-cont > p > a"
                item_link = "err"
                item = get_item_driver.find_element(By.CSS_SELECTOR, find_css_selector)
                item_link = item.get_attribute("href")
                self.pub_hot_deal_page(item_link)
            except Exception as e:
                error_logging(self.__class__.__name__, self.driver, e, f"fail get item links {find_css_selector}", item_link)
                
    @staticmethod
    def crawling(driver, item_link):
        driver.get(item_link)
        try: # 신고 처리, 보안 검사 등
            created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment = "err", "err", "err", "err", "err", "err", "err", "err"
            item_name = driver.find_element(By.CSS_SELECTOR, "#content > div > div.sub-content-wrap > div.left-con-wrap > div.common-view-wrap.market-info-view-wrap > div > dl > dt > div:nth-child(1) > h1").text.split()[2:]
            item_name = " ".join(item_name)
            table = driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.TAG_NAME, "tr")
            created_at = driver.find_element(By.CSS_SELECTOR, "#content > div > div.sub-content-wrap > div.left-con-wrap > div.common-view-wrap.market-info-view-wrap > div > dl > dt > div.util-area > p > span").text
            content = driver.find_element(By.CSS_SELECTOR, "#new_contents").text
            comment = list(map(lambda x: x.text, driver.find_elements(By.CSS_SELECTOR, "#content > div.sub-content-wrap > div.left-con-wrap > div.reply-wrap > div.reply-area > div.reply-list")))
            details = [row.text for row in rows]
            shopping_mall_link, shopping_mall, price, delivery, *_ = list(map(lambda x: "".join(x.split()[1:]), details))
            
        except Exception as e:
            error_logging("QUASAR_ZONE", driver, e, f"crawling error, {item_link}", item_link)
            
        finally:
            result = {
                "created_at" : created_at,
                "item_link" : item_link,
                "shopping_mall_link" : shopping_mall_link,
                "shopping_mall" : shopping_mall,
                "price" : price,
                "item_name" : item_name,
                "delivery" : delivery,
                "content" : content
            }
            return result

        
# shopping_mall이 tag되지 않은 채로 올라옴
class PPOM_PPU(PAGES):
    def __init__(self, pathfinder):
        self.site_name = PPOM_PPU_LINK
        super().__init__(pathfinder)
                
    def get_item_links(self):
        response = requests.get(self.site_name)
        soup = BeautifulSoup(response.content, "html.parser")
        for item in soup.find_all(class_= "baseList-thumb")[:20]:
            try:
                item_link = "https://www.ppomppu.co.kr/zboard/" + item.attrs["href"]
                item_link = item_link.replace("&&", "&")
                self.pub_hot_deal_page(item_link)
            except Exception as e:
                error_logging(self.__class__.__name__, self.driver, e, f"fail get item links {item}", item_link)
                
    @staticmethod
    def crawling(driver, item_link):
        driver.get(item_link)

        try: # 신고 처리, 보안 검사 등
            # item_name = driver.find_element(By.CSS_SELECTOR, "body > div.wrapper > div.contents > div.container > div > table:nth-child(9) > tbody > tr:nth-child(3) > td > table > tbody > tr > td:nth-child(5) > div > div.sub-top-text-box > font.view_title2").text
            # content = driver.find_element(By.CSS_SELECTOR, "body > div.wrapper > div.contents > div.container > div > table:nth-child(15) > tbody > tr:nth-child(1) > td > table > tbody > tr > td").text
            # comments = driver.find_element(By.ID, "quote").text
            # shopping_mall_link = driver.find_element(By.CSS_SELECTOR, "body > div.wrapper > div.contents > div.container > div > table:nth-child(9) > tbody > tr:nth-child(3) > td > table > tbody > tr > td:nth-child(5) > div > div.sub-top-text-box > div > a").get_attribute("href")
            # shopping_mall = driver.find_element(By.CSS_SELECTOR, "body > div.wrapper > div.contents > div.container > div > table:nth-child(9) > tbody > tr:nth-child(3) > td > table > tbody > tr > td:nth-child(5) > div > div.sub-top-text-box > font.view_title2 > span").text
            created_at, shopping_mall_link, shopping_mall, price, item_name, delivery, content, comment = "err", "err", "err", "err", "err", "err", "err", "err"
            item_name = driver.find_element(By.CSS_SELECTOR, "#topTitle > h1").text
            content = driver.find_element(By.CSS_SELECTOR, "body > div.wrapper > div.contents > div.container > div > table:nth-child(14) > tbody > tr:nth-child(1) > td > table > tbody > tr > td").text
            comment = driver.find_element(By.ID, "quote").text
            created_at = driver.find_element(By.CSS_SELECTOR, "#topTitle > div > ul > li:nth-child(2)").text.lstrip("등록일 ")
            shopping_mall_link = driver.find_element(By.CSS_SELECTOR, "#topTitle > div > ul > li.topTitle-link > a").text
            shopping_mall = driver.find_element(By.CSS_SELECTOR, "#topTitle > h1 > span.subject_preface.type2").text
            
        except Exception as e:
            error_logging("PPOM_PPU", driver, e, f"crawling error, {item_link}", item_link)
            
        finally:
            result = {
                "created_at" : created_at,
                "item_link" : item_link,
                "shopping_mall_link" : shopping_mall_link,
                "shopping_mall" : shopping_mall,
                "price" : price,
                "item_name" : item_name,
                "delivery" : delivery,
                "content" : content
            }
            return result

SITES = {
    "ARCA_LIVE" : ARCA_LIVE,
    "PPOM_PPU" : PPOM_PPU,
    "FM_KOREA" : FM_KOREA,
    "QUASAR_ZONE" : QUASAR_ZONE,
    "RULI_WEB" : RULI_WEB,
}

def start_scanning():
    pathfinder = PathFinder()
    quasar_zone = QUASAR_ZONE(pathfinder)
    ppom_ppu = PPOM_PPU(pathfinder)
    fm_korea = FM_KOREA(pathfinder)
    ruli_web = RULI_WEB(pathfinder)
    arca_live = ARCA_LIVE(pathfinder)
    while True:
        try:
            current = time.time()
            quasar_zone.get_item_links()
            logging.info(f" quasar zone {time.time() - current}")
            time.sleep(quasar_zone.refresh_delay)
            
            current = time.time()
            ppom_ppu.get_item_links()
            logging.info(f" ppomppu {time.time() - current}")
            time.sleep(ppom_ppu.refresh_delay)
            
            current = time.time()
            fm_korea.get_item_links()
            logging.info(f" fm korea {time.time() - current}")
            time.sleep(fm_korea.refresh_delay)
            
            current = time.time()
            ruli_web.get_item_links()
            logging.info(f" ruliweb {time.time() - current}")
            time.sleep(ruli_web.refresh_delay)
            
            current = time.time()
            arca_live.get_item_links()
            logging.info(f" arca live {time.time() - current}")
            time.sleep(arca_live.refresh_delay)
            
        except WebDriverException as e:
            logging.error(f"webdriver exception {e}")
            time.sleep(5)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # 현재 시간을 포맷팅
            error_log = {"error_log": e, "time": timestamp, "error_type": "while error"}
            screenshot_filename = f'error_screenshot/while error_{timestamp}.png'
            save_full_screenshot(pathfinder.driver, screenshot_filename)
            logging.error(error_log)
            
            try:
                with open(screenshot_filename, 'rb') as file:
                    response = client.files_upload_v2(
                        channel=SLACK_CHANNEL_ID,
                        file=file,
                        filename=os.path.basename(screenshot_filename),  # 파일 이름
                        initial_comment=error_log  # 업로드할 때 이미지 제목
                    )
                logging.info(f"File uploaded successfully: {response['file']['permalink']}")
            except Exception as e:
                logging.error(f"Error uploading file: {e}")
            
if __name__ == "__main__":
    logging.basicConfig(filename='scanner.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Start Scanning")
    connection = psycopg2.connect(
    dbname = DB_NAME,
    user = DB_USER,
    password = DB_PASSWORD,
    host = DB_HOST,
    port = DB_PORT
    )
    cursor = connection.cursor()

    page_insert_query = sql.SQL("""
        INSERT INTO pages (site_name_idx, item_link)
        VALUES (%s, %s)
    """)

    error_insert_query = sql.SQL("""
        INSERT INTO error (site_name_idx, error_log, timestamp, item_link)
        VALUES (%s, %s, %s, %s)
    """)

    client = WebClient(token=SLACK_TOKEN)

    producer = KafkaProducer(
        acks=0, # 메시지 전송 완료에 대한 체크
        compression_type='gzip', # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
        bootstrap_servers=['localhost:29092', 'localhost:39092', 'localhost:49092'], # 전달하고자 하는 카프카 브로커의 주소 리스트
        value_serializer=lambda x:json.dumps(x, default=str).encode('utf-8'), # 메시지의 값 직렬화
        key_serializer=lambda x:json.dumps(x, default=str).encode('utf-8') # 키의 값 직렬화
    )

    start_scanning()
