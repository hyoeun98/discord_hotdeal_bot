import requests
from bs4 import BeautifulSoup
import redis
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
import yaml
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
from logging.handlers import RotatingFileHandler
import tempfile
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright


# --- 로깅 설정 ---
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def setup_site_logger(site_name: str):
    """사이트별 로그 파일 핸들러를 동적으로 설정"""
    site_log_file = os.path.join(log_dir, f"scanner_{site_name.lower()}.log")

    # 기존 RotatingFileHandler 제거(중복 방지)
    for h in list(logger.handlers):
        if isinstance(h, RotatingFileHandler):
            logger.removeHandler(h)
            h.close()

    site_handler = RotatingFileHandler(
        site_log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    site_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(site_handler)

load_dotenv()

DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = os.environ["DB_PORT"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

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
    selector_path = os.path.join(os.path.dirname(__file__), 'selectors.yaml')
    with open(selector_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def normalize_comment_count(comment):
    match = re.search(r'\d+', comment)
    if match:
        return int(match.group())
    else:
        return 0

SELECTORS = load_selectors()
    
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
        
        logger.info(f"[{site_name}] new item links: {len(new_item_link)}개")

        
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
            logger.info(f"[{site_name}] Published {len(new_item_link)} items to Redis channel: {channel}")
        else:
            logger.info(f"[{site_name}] No new item links found")

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
        
        logger.info(f"[{site_name}] new trend links: {len(new_trend_item_link)}개")
        
        if new_trend_item_link:
            message = {
                "type": "trend",
                "site": site_name,
                "links": new_trend_item_link,
                "count": len(new_trend_item_link),
                "timestamp": datetime.now().isoformat(),
            }
            channel = f"trend:{site_name}"
            redis_client.publish(channel, json.dumps(message, ensure_ascii=False))
            logger.info(f"[{site_name}] Published {len(new_trend_item_link)} trend items to Redis channel: {channel}")
        else:
            logger.info("not found new trend item links")

    def db_get_item_links(self):
        conn = None
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
            logger.error(str(e))
            return []

        finally:
            if conn:
                conn.close()

    def db_get_trend_item_links(self):
        conn = None
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
            logger.error(str(e))
            return []

        finally:
            if conn:
                conn.close()

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
            
            response = page.content()
            
            browser.close()
        soup = BeautifulSoup(response, "html.parser")
        return soup
    
    def scanning(self):
        self.get_item_links()

        try:
            self.pub_item_links()
            self.pub_trend_item_links()
        except Exception as e:
            logger.error(f"fail pub item links {e}")

def capture_and_send_screenshot(driver_or_html, site_name, is_selenium=True):
    """
    Selenium 또는 BeautifulSoup HTML 기반 에러 로그 저장 및 Discord 전송
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if is_selenium:
        local_file = f"/tmp/{site_name}_{timestamp}.png"
        driver_or_html.save_screenshot(local_file)

        with open(local_file, "rb") as f:
            files = {"file": (local_file, f, "image/png")}
            payload = {"content": "에러 발생 스크린샷"}
            response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)
        logger.info(f"Selenium screenshot sent: {response.status_code}")
    
        page_source = driver_or_html.page_source
    
    else:
        page_source = driver_or_html
        logger.info("HTML content from BS4 ready to send.")
        response = None

    html_file_path = f"/tmp/{site_name}_{timestamp}.html"
    with open(html_file_path, "w", encoding="utf-8") as f:
        f.write(str(page_source))
    
    with open(html_file_path, "rb") as f:
        files = {"file": (html_file_path, f, "text/html")}
        payload = {"content": "에러 발생 페이지 소스"}
        html_response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)
    logger.info(f"HTML sent to Discord: {html_response.status_code}")

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
        logger.info(f"Error log saved to PostgreSQL: {site_name}")
    except Exception as e:
        logger.error(f"Failed to save error log to PostgreSQL: {e}")
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
        return urljoin(self.site_link, item_link)
    
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
            logger.error(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row["href"]
                item_link = self.get_merged_item_link(item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
            except Exception as e:
                logger.error(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break

class ARCA_LIVE(PAGES):
    def __init__(self, driver):
        self.site_link = ARCA_LIVE_LINK
        self.site_name = "ARCA_LIVE"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_merged_item_link(self, item_link):
        return urljoin(self.site_link, item_link)
    
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
            logger.error(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row["href"]
                item_link = self.get_merged_item_link(item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
            except Exception as e:
                logger.error(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break    
        
class FM_KOREA(PAGES):
    def __init__(self, driver):
        self.site_link = FM_KOREA_LINK
        self.site_name = "FM_KOREA"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]
    
    def get_merged_item_link(self, item_link):
        return urljoin(self.site_link, item_link)
    
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
            logger.error(f"{self.site_link} 접속 실패 {str(e)}")
            if soup:
                logger.error(soup)
            return
        
        for row in rows:
            try:
                item_link = row["href"]
                item_link = self.get_merged_item_link(item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
            except Exception as e:
                logger.error(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break    
    
class PPOM_PPU(PAGES):
    def __init__(self, driver):
        self.site_link = PPOM_PPU_LINK
        self.site_name = "PPOM_PPU"
        super().__init__(driver)
        self.selectors = SELECTORS[self.site_name]

    def get_merged_item_link(self, item_link):
        return urljoin(self.site_link, item_link)
    
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
            logger.error(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
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
                
            except Exception as e:
                logger.error(f"fail get item links {item_link} {e}")
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
            logger.error(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row.select_one("a")["href"]
                
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
            except Exception as e:
                logger.error(f"fail get item links {item_link} {e}")
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
            logger.error(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row.select_one("h3").select_one("a")["href"]
                item_link = urljoin(self.site_link, item_link)
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
            except Exception as e:
                logger.error(f"fail get item links {item_link} {e}")
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
            logger.error(f"{self.site_link} 접속 실패 {str(e)}")
            return
        
        for row in rows:
            try:
                item_link = row.select_one("a")["href"]
                self.item_link_list.append(item_link)
                comment_count = self.get_comment_count(row)
                    
                if self.is_trend_item(comment_count=comment_count):
                    self.trend_item_link_list.append(item_link)
                
            except Exception as e:
                logger.error(f"fail get item links {item_link} {e}")
                capture_and_send_screenshot(self.get_item_driver, self.__class__.__name__)
                break
            
def set_driver():
    chrome_options = webdriver.ChromeOptions()
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
    
    service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(10)
    driver.set_page_load_timeout(10)
    return driver
        
def main(site_name=None):
    """
    특정 사이트 이름을 인자로 받아 해당 사이트만 스캔
    예: python scanner.py RULI_WEB
    """
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
        logger.error(f"❌ Unknown site name: {site_name}")
        logger.error(f"Available sites: {', '.join(site_classes.keys())}")
        sys.exit(1)

    setup_site_logger(site_name)
    driver = set_driver()

    try:
        site_instance = site_classes[site_name](driver)
        
        logger.info(f"[Scanner] {site_name} 스캔 시작...")
        site_instance.scanning()
        
        new_item_links = site_instance.get_new_item_link()
        new_trend_links = site_instance.get_new_trend_item_link()
        
        logger.info(f"[Scanner] {site_name}: 새 아이템 {len(new_item_links)}개, 인기글 {len(new_trend_links)}개")
        
        result = {
            "site": site_name,
            "new_items": new_item_links,
            "new_trends": new_trend_links,
            "total_new": len(new_item_links),
            "total_trends": len(new_trend_links),
            "timestamp": datetime.now().isoformat()
        }
        
        print(json.dumps(result, ensure_ascii=False))
        
    except Exception as e:
        logger.error(f"⚠️ Error while scanning {site_name}: {e}")
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

    logger.info(f"✅ {site_name} scanning complete!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("⚠️ 사용법: python scanner.py [SITE_NAME]")
        logger.error("예시: python scanner.py RULI_WEB")
        sys.exit(1)

    site_name = sys.argv[1]
    main(site_name)
