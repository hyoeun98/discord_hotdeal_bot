import requests
from bs4 import BeautifulSoup
import redis
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
import yaml
import boto3
from abc import ABC, abstractmethod
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
import json
# import stealth_requests as requests
import curl_cffi
import re
import sys
import logging
import tempfile
from playwright.sync_api import sync_playwright


load_dotenv()

QUEUE_URL = os.environ["QUEUE_URL"]
REGION = os.environ.get("REGION", "ap-northeast-2")
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = os.environ["DB_PORT"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]
SNS_ARN = os.environ["SNS_ARN"]
TREND_SQS_URL = os.environ["TREND_SQS_URL"]

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)

ARCA_LIVE_LINK = "https://arca.live/b/hotdeal"
RULI_WEB_LINK = "https://bbs.ruliweb.com/market/board/1020?view=default"
PPOM_PPU_LINK = "https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu"
QUASAR_ZONE_LINK = "https://quasarzone.com/bbs/qb_saleinfo"
FM_KOREA_LINK = "https://www.fmkorea.com/hotdeal"
COOL_ENJOY_LINK = "https://coolenjoy.net/bbs/jirum"
EOMI_SAE_LINK = "https://eomisae.co.kr/fs"

USER_AGENT_HEADER = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

db_config = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": DB_PORT
    }

# Redis 클라이언트 초기화
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=True  # 자동으로 문자열로 디코딩
)

def load_selectors():
    with open("/home/hyoeun/hotdeal_bot/scanner/selectors.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def normalize_comment_count(comment):
    match = re.search(r'\d+', comment)
    if match:
        return int(match.group())
    else:
        return 0

SELECTORS = load_selectors()

sns = boto3.client('sns', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
    
class PAGES(ABC):
    """각 Page들의 SuperClass"""

    def __init__(self, driver):
        self.item_link_list = []
        self.trend_item_link_list = []
        self.get_item_driver = driver

    def get_new_item_link(self):
        db_item_links = self.db_get_item_links()
        new_item_link = list(set(self.item_link_list) - set(db_item_links))
        return new_item_link

    def pub_item_links(self):
        """Redis Pub/Sub으로 일반 핫딜 정보 Publish"""
        new_item_link = self.get_new_item_link()
        site_name = self.__class__.__name__
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{site_name}] new item links: {len(new_item_link)}개", file=sys.stderr)

        
        if new_item_link:
            message = {
                "type": "hotdeal",
                "site": site_name,
                "links": new_item_link,
                "count": len(new_item_link),
                "timestamp": datetime.now().isoformat()
            }
            
            # Redis Channel: hotdeal:{site_name}
            channel = f"hotdeal:{site_name}"
            redis_client.publish(channel, json.dumps(message, ensure_ascii=False))
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{site_name}] Published {len(new_item_link)} items to Redis channel: {channel}", file=sys.stderr)
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{site_name}] No new item links found", file=sys.stderr)

    def get_new_trend_item_link(self):
        db_trend_item_links = self.db_get_trend_item_links()
        new_trend_item_link = list(
            set(self.trend_item_link_list) - set(db_trend_item_links)
        )
        return new_trend_item_link

    def pub_trend_item_links(self):
        """Redis Pub/Sub으로 인기글 정보 Publish"""
        new_trend_item_link = self.get_new_trend_item_link()
        site_name = self.__class__.__name__
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{site_name}] new trend links: {len(new_trend_item_link)}개", file=sys.stderr)
        
        if new_trend_item_link:
            message_body = json.dumps(new_trend_item_link)
            num_item_links = str(len(new_trend_item_link))
            
            response = sqs.send_message(
                QueueUrl=TREND_SQS_URL,
                MessageBody=message_body,
                MessageAttributes={
                    "is_trend": {"DataType": "String", "StringValue": "1"},
                    "site_name": {"DataType": "String", "StringValue": site_name},
                    "num_item_links": {
                        "DataType": "String",
                        "StringValue": num_item_links,
                    },
                },
            )
            print("success pub trend item\n", response)
        else:
            print("not found new trend item links")
            
        #     # Redis Channel: trend:{site_name}
        #     channel = f"trend:{site_name}"
        #     redis_client.publish(channel, json.dumps(message, ensure_ascii=False))
        #     print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{site_name}] Published {len(new_trend_item_link)} trends to Redis channel: {channel}", file=sys.stderr)
        # else:
        #     print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{site_name}] No new trend links found", file=sys.stderr)

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
    def is_trend_item(self):
        pass

    def get_bs4_soup(self, link):
        response = curl_cffi.get(link, impersonate="chrome")
        soup = BeautifulSoup(response.text, "html.parser")
        return soup
    
    def get_bs4_soup_by_playwright(self, link):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            page.goto(link, wait_until='networkidle')
            # page.wait_for_load_state('domcontentloaded')
            
            response = page.content()
            
            browser.close()
        soup = BeautifulSoup(response, "html.parser")
        return soup
    
    def scanning(self):
        # with timer(f"{self.__class__.__name__} get item link"):
            # self.get_item_links()
            
        self.get_item_links()

        try:
            self.pub_item_links()
            self.pub_trend_item_links()
            ...
        except Exception as e:
            print(f"fail pub item links {e}")

def capture_and_send_screenshot(driver_or_html, site_name, is_selenium=True):
    """
    Selenium 또는 BeautifulSoup HTML 기반 에러 로그 저장 및 Discord 전송
    
    Args:
        driver_or_html: Selenium driver 객체 또는 HTML 문자열
        site_name: 사이트 이름
        is_selenium: True면 Selenium driver, False면 HTML 문자열 처리
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Selenium이면 스크린샷 저장
    if is_selenium:
        local_file = f"/tmp/{site_name}_{timestamp}.png"
        driver_or_html.save_screenshot(local_file)

        # Discord로 전송
        with open(local_file, "rb") as f:
            files = {"file": (local_file, f, "image/png")}
            payload = {"content": "에러 발생 스크린샷"}
            response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)
        print(f"Selenium screenshot sent: {response.status_code}")
    
        page_source = driver_or_html.page_source
    
    else:
        # BS4 HTML 문자열이면 스크린샷 없음, HTML만 저장
        page_source = driver_or_html
        print("HTML content from BS4 ready to send.")
        response = None

    # HTML 파일 저장 및 Discord 전송
    html_file_path = f"/tmp/{site_name}_{timestamp}.html"
    with open(html_file_path, "w", encoding="utf-8") as f:
        f.write(str(page_source))
    
    with open(html_file_path, "rb") as f:
        files = {"file": (html_file_path, f, "text/html")}
        payload = {"content": "에러 발생 페이지 소스"}
        html_response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)
    print(f"HTML sent to Discord: {html_response.status_code}")

    # PostgreSQL에 저장
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO scanner_error_logs (site_name, page_source, created_at, error_type)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (site_name, page_source, timestamp, "scanning error"))
        conn.commit()
        print(f"Error log saved to PostgreSQL: {site_name}")
    except Exception as e:
        print(f"Failed to save error log to PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()
            
class QUASAR_ZONE(PAGES):
    def __init__(self, driver):
        self.site_link = QUASAR_ZONE_LINK
        self.site_name = "QUASAR_ZONE"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_merged_item_link(self, item_link):
        merged_item_link = self.site_link.rstrip("/bbs/qb_saleinfo") + item_link
        return merged_item_link
    
    def get_comment_count(self, item):
        s_config = self.selectors['get_comment_count']
        try:
            comment_count = item.select_one(s_config['comment_count_selector'])
            comment_count = normalize_comment_count(comment_count.get_text())
        
        except Exception:
            comment_count = 0
        
        finally:
            return comment_count
            
    def is_trend_item(self, **kwargs):
        comment_count = kwargs["comment_count"]
        if comment_count >= 30:
            return True
        return False
        
    
    def get_item_links(self):
        s_config = self.selectors['get_item_links']
        try:
            soup = self.get_bs4_soup(self.site_link)
            board = soup.select_one(s_config["find_board_selector"])
            rows = board.select(s_config["find_row_selector"])

        except Exception as e:
            print(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row["href"]
                item_link = self.get_merged_item_link(item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
                logging.info(f"{item_link} comment : {comment_count}")
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break

class ARCA_LIVE(PAGES):
    def __init__(self, driver):
        self.site_link = ARCA_LIVE_LINK
        self.site_name = "ARCA_LIVE"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_merged_item_link(self, item_link):
        merged_item_link = self.site_link.rstrip("/b/hotdeal") + item_link
        merged_item_link = merged_item_link.replace("liv", "live")
        return merged_item_link
    
    def get_comment_count(self, item):
        s_config = self.selectors['get_comment_count']
        try:
            comment_count = item.select_one(s_config['comment_count_selector'])
            comment_count = normalize_comment_count(comment_count.get_text())
        
        except Exception:
            comment_count = 0
        
        finally:
            return comment_count
    
    def is_trend_item(self, **kwargs):
        comment_count = kwargs["comment_count"]
        if comment_count >= 30:
            return True
        return False
    
    def get_item_links(self):
        s_config = self.selectors['get_item_links']
        try:
            soup = self.get_bs4_soup(self.site_link)
            board = soup.select_one(s_config["find_board_selector"])
            rows = board.select(s_config["find_row_selector"])

        except Exception as e:
            print(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row["href"]
                item_link = self.get_merged_item_link(item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
                logging.info(f"{item_link} comment : {comment_count}")
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break    
        
class FM_KOREA(PAGES):
    def __init__(self, driver):
        self.site_link = FM_KOREA_LINK
        self.site_name = "FM_KOREA"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]
    
    def get_merged_item_link(self, item_link):
        merged_item_link = self.site_link.rstrip("/hotdeal") + item_link
        return merged_item_link
    
    def get_comment_count(self, item):
        s_config = self.selectors['get_comment_count']
        try:
            comment_count = item.select_one(s_config['comment_count_selector'])
            comment_count = normalize_comment_count(comment_count.get_text())
        
        except Exception:
            comment_count = 0
        
        finally:
            return comment_count
    
    def is_trend_item(self, **kwargs):
        comment_count = kwargs["comment_count"]
        if comment_count >= 30:
            return True
        return False
    
    def get_item_links(self):
        s_config = self.selectors['get_item_links']
        try:
            soup = self.get_bs4_soup_by_playwright(self.site_link)
            board = soup.select_one(s_config["find_board_selector"])
            rows = board.select(s_config["find_row_selector"])

        except Exception as e:
            print(f"{self.site_link} 접속 실패 {str(e)}")
            if soup:
                print(soup)
            return
        
        for row in rows:
            try:
                item_link = row["href"]
                item_link = self.get_merged_item_link(item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
                logging.info(f"{item_link} comment : {comment_count}")
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break    
    
class PPOM_PPU(PAGES):
    def __init__(self, driver):
        self.site_link = PPOM_PPU_LINK
        self.site_name = "PPOM_PPU"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_merged_item_link(self, item_link):
        merged_item_link = self.site_link.rstrip("zboard.php?id=ppomppu") + item_link
        return merged_item_link
    
    def get_comment_count(self, item):
        s_config = self.selectors['get_comment_count']
        try:
            comment_count = item.select_one(s_config['comment_count_selector'])
            comment_count = normalize_comment_count(comment_count.get_text())
        
        except Exception:
            comment_count = 0
        
        finally:
            return comment_count
    
    def is_trend_item(self, **kwargs):
        comment_count = kwargs["comment_count"]
        if comment_count >= 30:
            return True
        return False
    
    def get_item_links(self):
        s_config = self.selectors['get_item_links']
        try:
            soup = self.get_bs4_soup(self.site_link)
            board = soup.select_one(s_config["find_board_selector"])
            rows = board.select(s_config["find_row_selector"])

        except Exception as e:
            print(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        # 하단 8개는 광고
        for row in rows[:-8]:
            try:
                item_link = row.select_one("a")["href"]
                if item_link.count("zboard") > 1:
                    continue
                item_link = self.get_merged_item_link(item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
                logging.info(f"{item_link} comment : {comment_count}")
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break    

class COOL_ENJOY(PAGES):
    def __init__(self, driver):
        self.site_link = COOL_ENJOY_LINK
        self.site_name = "COOL_ENJOY"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_comment_count(self, item):
        s_config = self.selectors['get_comment_count']
        try:
            # print(item)
            comment_count = item.select_one(s_config['comment_count_selector'])
            comment_count = normalize_comment_count(comment_count.get_text())
        
        except Exception:
            comment_count = 0
        
        finally:
            return comment_count
    
    def is_trend_item(self, **kwargs):
        comment_count = kwargs["comment_count"]
        if comment_count >= 30:
            return True
        return False
    
    def get_item_links(self):
        s_config = self.selectors['get_item_links']
        try:
            soup = self.get_bs4_soup(self.site_link)
            board = soup.select_one(s_config["find_board_selector"])
            rows = board.select(s_config["find_row_selector"])

        except Exception as e:
            print(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row.select_one("a")["href"]
                
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
                logging.info(f"{item_link} comment : {comment_count}")
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break
            
class EOMI_SAE(PAGES):
    def __init__(self, driver):
        self.site_link = EOMI_SAE_LINK
        self.site_name = "EOMI_SAE"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_comment_count(self, item):
        s_config = self.selectors['get_comment_count']
        try:
            # print(item)
            comment_count = item.select_one(s_config['comment_count_selector'])
            comment_count = normalize_comment_count(comment_count.get_text())
        
        except Exception:
            comment_count = 0
        
        finally:
            return comment_count
    
    def is_trend_item(self, **kwargs):
        comment_count = kwargs["comment_count"]
        if comment_count >= 30:
            return True
        return False
    
    def get_item_links(self):
        s_config = self.selectors['get_item_links']
        try:
            soup = self.get_bs4_soup(self.site_link)
            board = soup.select_one(s_config["find_board_selector"])
            rows = board.select(s_config["find_row_selector"])
        except Exception as e:
            print(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row.select_one("h3").select_one("a")["href"]
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
                logging.info(f"{item_link} comment : {comment_count}")
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break
            
class RULI_WEB(PAGES):
    def __init__(self, driver):
        self.site_link = RULI_WEB_LINK
        self.site_name = "RULI_WEB"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_comment_count(self, item):
        s_config = self.selectors['get_comment_count']
        try:
            comment_count = item.select_one(s_config['comment_count_selector'])
            comment_count = normalize_comment_count(comment_count.get_text())
        
        except Exception:
            comment_count = 0
        
        finally:
            return comment_count
    
    def is_trend_item(self, **kwargs):
        comment_count = kwargs["comment_count"]
        if comment_count >= 30:
            return True
        return False
    
    def get_item_links(self):
        s_config = self.selectors['get_item_links']
        try:
            soup = self.get_bs4_soup(self.site_link)
            board = soup.select_one(s_config["find_board_selector"])
            rows = board.select(s_config["find_row_selector"])
        except Exception as e:
            print(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row.select_one("a")["href"]
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
                logging.info(f"{item_link} comment : {comment_count}")
                
            except Exception as e:
                print(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break
            
def set_driver():
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.binary_location = "/usr/bin/chrome"
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
    tmp_profile = tempfile.mkdtemp()
    
    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # stealth(driver,
    #     languages=["en-US", "en"],
    #     vendor="Google Inc.",
    #     platform="Win32",
    #     webgl_vendor="Intel Inc.",
    #     renderer="Intel Iris OpenGL Engine",
    #     fix_hairline=True,
    # )
    driver.implicitly_wait(10)
    driver.set_page_load_timeout(10)
    return driver

@contextmanager
def timer(name: str):
    t0 = time.time()
    try:
        yield
    finally:
        print(f"{name} done in {time.time() - t0:.3f} s")
        
def main(site_name=None):
    """
    특정 사이트 이름을 인자로 받아 해당 사이트만 스캔
    예: python scanner.py RULI_WEB
    """
    driver = set_driver()

    # 사이트 이름 → 클래스 매핑
    site_classes = {
        "QUASAR_ZONE": QUASAR_ZONE,
        "PPOM_PPU": PPOM_PPU,
        "FM_KOREA": FM_KOREA,
        "ARCA_LIVE": ARCA_LIVE,
        "COOL_ENJOY": COOL_ENJOY,
        "EOMI_SAE": EOMI_SAE,
        "RULI_WEB": RULI_WEB,
    }

    if site_name not in site_classes:
        # 에러 메시지는 stderr로
        print(f"❌ Unknown site name: {site_name}", file=sys.stderr)
        print(f"Available sites: {', '.join(site_classes.keys())}", file=sys.stderr)
        driver.quit()
        sys.exit(1)

    try:
        site_instance = site_classes[site_name](driver)
        
        # 스캔 실행 (디버그 로그는 stderr로)
        # print(f"[Scanner] {site_name} 스캔 시작...", file=sys.stderr)
        site_instance.scanning()
        
        # 새로운 아이템만 추출
        new_item_links = site_instance.get_new_item_link()
        new_trend_links = site_instance.get_new_trend_item_link()
        
        # print(f"[Scanner] {site_name}: 새 아이템 {len(new_item_links)}개, 인기글 {len(new_trend_links)}개", file=sys.stderr)
        
        # 결과를 JSON으로 stdout에 출력 (Airflow가 이걸 읽음)
        result = {
            "site": site_name,
            "new_items": new_item_links,
            "new_trends": new_trend_links,
            "total_new": len(new_item_links),
            "total_trends": len(new_trend_links),
            "timestamp": datetime.now().isoformat()
        }
        
        # 이것만 stdout으로 출력 (Airflow가 파싱)
        print(json.dumps(result, ensure_ascii=False))
        
    except Exception as e:
        print(f"⚠️ Error while scanning {site_name}: {e}", file=sys.stderr)
        # 에러 발생 시 빈 결과 반환
        print(json.dumps({
            "site": site_name,
            "new_items": [],
            "new_trends": [],
            "total_new": 0,
            "total_trends": 0,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }))
        sys.exit(1)
    finally:
        driver.quit()

    # print(f"✅ {site_name} scanning complete!", file=sys.stderr)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("⚠️ 사용법: python scanner.py [SITE_NAME]")
        print("예시: python scanner.py RULI_WEB")
        sys.exit(1)

    site_name = sys.argv[1]
    main(site_name)
