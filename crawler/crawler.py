import json
import asyncio
import aiohttp
import re
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from datetime import datetime
from bs4 import BeautifulSoup
import yaml
from dotenv import load_dotenv
import tempfile
import redis.asyncio as redis
from curl_cffi import AsyncSession
import logging
from logging.handlers import RotatingFileHandler
from playwright.async_api import async_playwright

# --- Logging 설정 ---
log_dir = "/home/hyoeun/hotdeal_bot/crawler/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "crawler.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

load_dotenv()

DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

ARCA_LIVE_LINK = "https://arca.live/b/hotdeal"
RULI_WEB_LINK = "https://bbs.ruliweb.com/market/board/1020?view=default"
PPOM_PPU_LINK = "https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu"
QUASAR_ZONE_LINK = "https://quasarzone.com/bbs/qb_saleinfo"
FM_KOREA_LINK = "https://www.fmkorea.com/hotdeal"
COOL_ENJOY_LINK = "https://coolenjoy.net/bbs/jirum"
EOMI_SAE_LINK = "https://eomisae.co.kr/fs"

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
REDIS_CHANNELS = [
    "hotdeal:PPOM_PPU",
    "hotdeal:RULI_WEB",
    "hotdeal:ARCA_LIVE",
    "hotdeal:COOL_ENJOY",
    "hotdeal:QUASAR_ZONE",
    "hotdeal:FM_KOREA",
    "hotdeal:EOMI_SAE",
]

# Redis 클라이언트 초기화
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=True
)

def load_selectors():
    with open("/home/hyoeun/hotdeal_bot/crawler/selectors.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


SELECTORS = load_selectors()


async def send_discord_screenshot(file_path, content_text):
    """Discord로 스크린샷 전송 (비동기)"""
    try:
        async with aiohttp.ClientSession() as session:
            with open(file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("file", f, filename=os.path.basename(file_path))
                form.add_field("content", content_text)
                
                async with session.post(DISCORD_WEBHOOK, data=form) as resp:
                    logger.info(f"Discord response: {resp.status}")
    except Exception as e:
        logger.error(f"Failed to send Discord notification: {e}")

class PAGES:
    """각 Page들의 SuperClass"""

    def __init__(self):
        self.refresh_delay = 30
        self.curl_session = None
        
        self.playwright = None
        self.browser = None
        
        # ✅ 추가: Playwright 사용 클래스들을 위한 세마포어 (QUASAR_ZONE은 Playwright 사용)
        self.sem = asyncio.Semaphore(1)  # 단일 브라우저 처리 보장

    async def __aenter__(self):
        """async context manager 진입: HTTP 세션 및 Playwright 브라우저 초기화"""
        self.curl_session = AsyncSession()
        
        # ✅ 수정: 컨텍스트 진입 시 Playwright 브라우저를 초기화합니다.
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--single-process",
                ],
            )
        except Exception:
            logger.exception("Failed to launch Playwright browser in __aenter__")
            
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """async context manager 종료 시 브라우저도 함께 종료"""
        if self.curl_session:
            await self.curl_session.close()
            
        # [추가] 봇이 종료될 때 브라우저 닫기
        if self.browser:
            await self.browser.close()
            self.browser = None # 💡 명시적 초기화
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None # 💡 명시적 초기화
            
    async def get_bs4_soup(self, link):
        try:
            async with AsyncSession() as s:
                response = await s.get(link, impersonate="chrome")
                return BeautifulSoup(response.content, "html.parser")
        except Exception as e:
            logger.error("❌ Failed to fetch URL %s: %s", link, e)
            return None
    
    async def pub_item_links(self, message):
        """Redis Pub/Sub으로 Crawl 정보 Publish"""
        try:
            crawled_site = self.__class__.__name__
            payload = {
                "type": "crawl",
                "site": crawled_site,
                "data": message,
            }
            channel = f"crawl:{crawled_site}"
            await redis_client.publish(channel, json.dumps(payload, ensure_ascii=False))
            return True
            
        except Exception as e:
            logger.error(f"❌ Redis publish error: {type(e).__name__}: {e}")
            return False


class QUASAR_ZONE(PAGES):
    def __init__(self):
        super().__init__()
        self.site_link = QUASAR_ZONE_LINK
        self.site_name = "QUASAR_ZONE"
        self.selectors = SELECTORS[self.site_name]["crawling_selectors"]
        
    async def crawling(self, item_link_list):
        async def process_link(item_link):
            soup = None
            
            # ✅ 세마포어 적용: Playwright를 사용하는 작업이 순차적으로 실행되도록 강제
            soup = await self.get_bs4_soup(item_link)
            if not soup:
                return
            
            try:
                # logger.error(soup)
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err"
                    
                item_name_element = soup.select_one(self.selectors["item_name_css"])
                if item_name_element:
                    item_name_text = item_name_element.get_text(strip=True)
                    item_name = " ".join(item_name_text.split()[1:]) if item_name_text else "err"

                created_at_element = soup.select_one(self.selectors["created_at_css"])
                created_at = created_at_element.get_text(strip=True)

                content_element = soup.select_one(self.selectors["content_css"])

                if content_element:
                    content = re.sub(r"\s+", r" ", content_element.get_text(separator=" ", strip=True))
                else:
                    content = "err"

                
                category_element = soup.select_one(self.selectors["category_selector"])
                category = category_element.get_text(strip=True)

                table = soup.select_one(self.selectors["table_css"])
                table_info = re.sub(r'\s+', ' ', table.get_text()).strip()
                shopping_mall_link = re.search(r'커미션을 지급받습니다\.\s+(.+?)\s+판매처', table_info).group(1).strip()
                shopping_mall = re.search(r'판매처\s+(.+?)\s+가격', table_info).group(1).strip()
                price = re.search(r'가격\s+(.+?)\s+배송비/직배', table_info).group(1).strip()
                delivery = re.search(r'배송비/직배\s+(.+)', table_info).group(1).strip()
                
            except Exception as e:
                logger.error(f"❌ {self.site_name} fail: {item_link} - {str(e)}")

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

                logger.info(f"[{datetime.now().strftime('%Y%m%d_%H%M%S')}] {self.site_name}: {result}")
                await self.pub_item_links(result)
                
        await asyncio.gather(*(process_link(link) for link in item_link_list))

class ARCA_LIVE(PAGES):
    # ... (생략: 기존 코드와 동일)

    def __init__(self):
        super().__init__()
        self.site_link = ARCA_LIVE_LINK
        self.site_name = "ARCA_LIVE"
        self.selectors = SELECTORS[self.site_name]["crawling_selectors"]

    async def crawling(self, item_link_list):
        async def process_link(item_link):
            soup = await self.get_bs4_soup(item_link)
            if not soup:
                return
            
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err"
                
                rows = soup.select("table tr")
                if len(rows) >= 5:
                    shopping_mall_link = rows[0].select("td")[1].get_text(strip=True) if rows[0].select("td") else "err"
                    shopping_mall = rows[1].select("td")[1].get_text(strip=True) if rows[1].select("td") else "err"
                    item_name = rows[2].select("td")[1].get_text(strip=True) if rows[2].select("td") else "err"
                    price = rows[3].select("td")[1].get_text(strip=True) if rows[3].select("td") else "err"
                    delivery = rows[4].select("td")[1].get_text(strip=True) if rows[4].select("td") else "err"
                
                content_selector = self.selectors.get("content_selector", "")
                content_elements = soup.select(content_selector)
                content = " ".join([x.text for x in content_elements])
                
                comment_list = self.selectors.get("comment_list_selector")
                
                created_at_selector = self.selectors.get("created_at_selector", "")
                created_at = soup.select_one(created_at_selector).text if created_at_selector and soup.select_one(created_at_selector) else "err"
                
                category_selector = self.selectors.get("category_selector", "")
                category = soup.select_one(category_selector).text if category_selector and soup.select_one(category_selector) else "err"
                
            except Exception as e:
                logger.error(f"❌ {self.site_name} fail: {item_link} - {str(e)}")

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
                logger.info(f"[{datetime.now().strftime('%Y%m%d_%H%M%S')}] {self.site_name}: {result}")
                await self.pub_item_links(result)
                
        await asyncio.gather(*(process_link(link) for link in item_link_list))

class RULI_WEB(PAGES):
    # ... (생략: 기존 코드와 동일)

    def __init__(self):
        super().__init__()
        self.site_link = RULI_WEB_LINK
        self.site_name = "RULI_WEB"
        self.selectors = SELECTORS[self.site_name]["crawling_selectors"]

    async def crawling(self, item_link_list):
        async def process_link(item_link):
            soup = await self.get_bs4_soup(item_link)
            if not soup:
                return
            
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err"
                
                item_name_selector = self.selectors.get("item_name_selector", "")
                item_name = soup.select_one(item_name_selector).text if item_name_selector and soup.select_one(item_name_selector) else "err"
                
                shopping_mall_match = re.findall(r"\[.+\]", item_name)
                if shopping_mall_match:
                    shopping_mall = shopping_mall_match[0]
                    
                created_at_selector = self.selectors.get("created_at_selector", "")
                created_at = soup.select_one(created_at_selector).text if created_at_selector and soup.select_one(created_at_selector) else "err"
                
                content_selector = self.selectors.get("content_selector", "")
                content = soup.select_one(content_selector).text if content_selector and soup.select_one(content_selector) else "err"
                
                comment_selector = self.selectors.get("comment_selector", "")
                comment = [c.text for c in soup.select(comment_selector)]
                
                shopping_mall_link_selector = self.selectors.get("shopping_mall_link_selector", "")
                shopping_mall_link = soup.select_one(shopping_mall_link_selector).text if shopping_mall_link_selector and soup.select_one(shopping_mall_link_selector) else "err"
                
                category_selector = self.selectors.get("category_selector", "")
                category = soup.select_one(category_selector).text if category_selector and soup.select_one(category_selector) else "err"

            except Exception as e:
                logger.error(f"❌ {self.site_name} fail: {item_link} - {str(e)}")

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
                logger.info(f"[{datetime.now().strftime('%Y%m%d_%H%M%S')}] {self.site_name}: {result}")
                await self.pub_item_links(result)

        await asyncio.gather(*(process_link(link) for link in item_link_list))
        
class FM_KOREA(PAGES):
    # ... (생략: 기존 코드와 동일)

    def __init__(self):
        super().__init__()
        self.site_link = FM_KOREA_LINK
        self.site_name = "FM_KOREA"
        self.selectors = SELECTORS[self.site_name]["crawling_selectors"]

    async def crawling(self, item_link_list):
        async def process_link(item_link):
            soup = await self.get_bs4_soup(item_link)
            if not soup:
                return
            
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err"
                
                rows_selector = self.selectors.get("rows_selector", "")
                rows = soup.select(rows_selector)
                if len(rows) >= 5:
                    shopping_mall_link = rows[0].text if rows[0] else "err"
                    shopping_mall = rows[1].text if rows[1] else "err"
                    item_name = rows[2].text if rows[2] else "err"
                    price = rows[3].text if rows[3] else "err"
                    delivery = rows[4].text if rows[4] else "err"

                content_selector = self.selectors.get("content_selector", "")
                content = (
                    re.sub(r"\s+", " ",
                        soup.select_one(content_selector).get_text(separator="\n", strip=True)
                    )
                    if content_selector and soup.select_one(content_selector) else "err"
                )
                
                comment_selector = self.selectors.get("comment_selector", "")
                comments = soup.select(comment_selector)
                comment = []
                for c in comments:
                    for a in c.find_all("a"):
                        a.decompose()
                    comment.append(c.get_text(strip=True))

                created_at_selector = self.selectors.get("created_at_selector", "")
                created_at = soup.select_one(created_at_selector).text if created_at_selector and soup.select_one(created_at_selector) else "err"
                
                category_selector = self.selectors.get("category_selector", "")
                category = soup.select_one(category_selector).text if category_selector and soup.select_one(category_selector) else "err"
                
            except Exception as e:
                logger.error(f"❌ {self.site_name} fail: {item_link} - {str(e)}")

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
                logger.info(f"[{datetime.now().strftime('%Y%m%d_%H%M%S')}] {self.site_name}: {result}")
                await self.pub_item_links(result)

        await asyncio.gather(*(process_link(link) for link in item_link_list))

class PPOM_PPU(PAGES):
    # ... (생략: 기존 코드와 동일)

    def __init__(self):
        super().__init__()
        self.site_link = PPOM_PPU_LINK
        self.site_name = "PPOM_PPU"
        self.selectors = SELECTORS[self.site_name]["crawling_selectors"]

    async def crawling(self, item_link_list):
        async def process_link(item_link):
            soup = await self.get_bs4_soup(item_link)
            if not soup:
                return
            
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err"
                
                item_name = soup.select_one(self.selectors["item_name_css"]).text
                if not re.search(r"[가-힣a-zA-Z]", item_name):
                    raise ValueError(f"유효하지 않은 상품명: {item_name}, {item_link}")
                content = soup.select_one(self.selectors["content_selector"]).get_text(strip=True)
                comment = [i.get_text(strip=True) for i in soup.select(self.selectors["comment_id"])]
                    
                created_at = soup.select_one(self.selectors["created_at_selector"]).text.lstrip("등록일 ")
                shopping_mall_link = soup.select_one(self.selectors["shopping_mall_link_selector"]).text
                shopping_mall = soup.select_one(self.selectors["shopping_mall_selector"]).text
                item_name = re.sub(r"^\[[^]]*\]\s*", "", item_name, count=1)
                
            except Exception as e:
                if item_name != "err" and shopping_mall == "err":
                    shopping_mall_match = re.match(r"\[.+\]", item_name)
                    if shopping_mall_match:
                        shopping_mall = shopping_mall_match[0]
                logger.error(f"❌ {self.site_name} fail: {item_link} - {str(e)}")

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
                logger.info(f"[{datetime.now().strftime('%Y%m%d_%H%M%S')}] {self.site_name}: {result}")
                await self.pub_item_links(result)

        await asyncio.gather(*(process_link(link) for link in item_link_list))
        
class COOL_ENJOY(PAGES):
    # ... (생략: 기존 코드와 동일)

    def __init__(self):
        super().__init__()
        self.site_link = COOL_ENJOY_LINK
        self.site_name = "COOL_ENJOY"
        self.selectors = SELECTORS[self.site_name]["crawling_selectors"]

    async def crawling(self, item_link_list):
        async def process_link(item_link):
            soup = await self.get_bs4_soup(item_link)
            if not soup:
                return
            
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err"
                
                title_text_selector = self.selectors.get("title_text_selector", "")
                title_text_elem = soup.select_one(title_text_selector)
                title_text = title_text_elem.get_text(strip=True) if title_text_elem else ""
                
                if title_text:
                    split_result = re.split(r"분류\|\s*\t", title_text)
                    if len(split_result) == 2:
                        category, title_text = split_result
                    
                    match_result = re.match(r"^\[([^\]]+)\]\s*(.*)", title_text)
                    if match_result:
                        shopping_mall, item_name = match_result.groups()
                
                content_selector = self.selectors.get("content_selector", "")
                content_elem = soup.select_one(content_selector)
                content = content_elem.get_text(separator="\n") if content_elem else "err"
                content = re.sub(r"\s+", " ", content).strip()
                
                comment_id = self.selectors.get("comment_selector", "")
                comments = soup.select(comment_id)
                comment = [c.get_text(strip=True) for c in comments]
                
                created_at_selector = self.selectors.get("created_at_selector", "")
                created_at_elem = soup.select_one(created_at_selector)
                created_at = created_at_elem.text if created_at_elem else "err"
                
                shopping_mall_link_selector = self.selectors.get("shopping_mall_link_selector", "")
                shopping_mall_link_elem = soup.select_one(shopping_mall_link_selector)
                if shopping_mall_link_elem:
                    for span in shopping_mall_link_elem.select("span"):
                        span.decompose()
                    shopping_mall_link = shopping_mall_link_elem.get_text(strip=True)
                
            except Exception as e:
                logger.error(f"❌ {self.site_name} fail: {item_link} - {str(e)}")

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
                logger.info(f"[{datetime.now().strftime('%Y%m%d_%H%M%S')}] {self.site_name}: {result}")
                await self.pub_item_links(result)

        await asyncio.gather(*(process_link(link) for link in item_link_list))

class EOMI_SAE(PAGES):
    # ... (생략: 기존 코드와 동일)

    def __init__(self):
        super().__init__()
        self.site_link = EOMI_SAE_LINK
        self.site_name = "EOMI_SAE"
        self.selectors = SELECTORS[self.site_name]["crawling_selectors"]

    async def crawling(self, item_link_list):
        async def process_link(item_link):
            soup = await self.get_bs4_soup(item_link)
            if not soup:
                return
            
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err"
                
                item_name_selector = self.selectors.get("item_name_selector", "")
                item_name_elem = soup.select_one(item_name_selector)
                item_name = item_name_elem.text if item_name_elem else "err"
                
                category_selector = self.selectors.get("category_selector", "")
                category_elem = soup.select_one(category_selector)
                category = category_elem.text if category_elem else "err"
                
                content_selector = self.selectors.get("content_selector", "")
                content_elem = soup.select_one(content_selector)
                content = content_elem.get_text(strip=True) if content_elem else "err"
                
                comment_selector = self.selectors.get("comment_selector", "")
                comments = soup.select(comment_selector)
                comment = [c.get_text(strip=True) for c in comments]
                
                created_at_selector = self.selectors.get("created_at_selector", "")
                created_at_elems = soup.select(created_at_selector)
                created_at = created_at_elems[-1].text if created_at_elems else "err"
                
                shopping_mall_link_selector = self.selectors.get("shopping_mall_link_selector", "")
                shopping_mall_link_elem = soup.select_one(shopping_mall_link_selector)
                shopping_mall_link = shopping_mall_link_elem.text if shopping_mall_link_elem else "err"

            except Exception as e:
                logger.error(f"❌ {self.site_name} fail: {item_link} - {str(e)}")

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
                logger.info(f"[{datetime.now().strftime('%Y%m%d_%H%M%S')}] {self.site_name}: {result}")
                await self.pub_item_links(result)

        await asyncio.gather(*(process_link(link) for link in item_link_list))

# 사이트 클래스 매핑
SITE_CLASSES = {
    "QUASAR_ZONE": QUASAR_ZONE,
    "PPOM_PPU": PPOM_PPU,
    "FM_KOREA": FM_KOREA,
    "ARCA_LIVE": ARCA_LIVE,
    "COOL_ENJOY": COOL_ENJOY,
    "EOMI_SAE": EOMI_SAE,
    "RULI_WEB": RULI_WEB,
}


# --- 메시지 처리 ---
async def process_message(msg):
    try:
        # dict or JSON string 모두 처리
        if isinstance(msg, (dict, list)):
            data = msg
        else:
            data = json.loads(msg)

        message_type = data.get("type")
        site_name = data.get("site")
        links = data.get("links", [])
        count = data.get("count", 0)

        if message_type != "hotdeal":
            logger.error(f"⚠️ Ignoring non-hotdeal message: {message_type}")
            return

        if not site_name or not links:
            logger.error(f"⚠️ Invalid message format: {data}")
            return

        if site_name not in SITE_CLASSES:
            logger.error(f"❌ Unknown site: {site_name}")
            return

        now_time = datetime.now().strftime("%H:%M:%S")
        logger.info(f"[{now_time}] 🚀 {site_name}: processing {count} items...")

        # async context manager로 세션 관리
        crawler = SITE_CLASSES[site_name]()
        async with crawler:
            await crawler.crawling(links)

        now_time = datetime.now().strftime("%H:%M:%S")
        logger.info(f"[{now_time}] ✅ {site_name} finished.")

    except Exception as e:
        logger.error(f"❌ Error processing message: {e} | Raw={msg}")


# --- Redis 구독 generator ---
async def redis_subscribe(channel):
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)
    logger.info(f"📡 Subscribed to {channel}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                yield message["data"]
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()


# --- 각 채널 전용 worker ---
async def worker(channel):
    async for msg in redis_subscribe(channel):
        await process_message(msg)

# --- 메인 ---
async def main():
    tasks = [worker(ch) for ch in REDIS_CHANNELS]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
