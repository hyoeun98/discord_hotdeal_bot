import json
import requests
import re
import boto3
import ast
import os
from selenium_stealth import stealth
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import yaml

QUEUE_URL = os.environ["QUEUE_URL"]
REGION = os.environ.get("REGION", "ap-northeast-2")
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

ARCA_LIVE_LINK = "https://arca.live/b/hotdeal"
RULI_WEB_LINK = "https://bbs.ruliweb.com/market/board/1020?view=default"
PPOM_PPU_LINK = "https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu"
QUASAR_ZONE_LINK = "https://quasarzone.com/bbs/qb_saleinfo"
FM_KOREA_LINK = "https://www.fmkorea.com/hotdeal"
COOL_ENJOY_LINK = "https://coolenjoy.net/bbs/jirum"
EOMI_SAE_LINK = "https://eomisae.co.kr/fs"


def load_crawler_selectors():
    # Path updated to "selectors.yml" to be relative to main.py's execution directory (/var/task in Docker)
    with open("selectors.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


CRAWLER_SELECTORS = load_crawler_selectors()

session = requests.Session()
retry = Retry(connect=2, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


def capture_and_send_screenshot(driver, file_name):
    """화면 캡처 후 Discord로 직접 전송"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_file = f"/tmp/{file_name}_{timestamp}.png"

    # 화면 캡처
    driver.save_screenshot(local_file)

    # Discord로 전송
    with open(local_file, "rb") as f:
        files = {"file": (local_file, f, "image/png")}
        payload = {"content": "에러 발생 스크린샷"}
        response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)

    page_source = driver.page_source

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"/tmp/{timestamp}_page.html"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(page_source)

    with open(file_path, "rb") as f:
        files = {"file": (file_path, f, "text/html")}
        payload = {"content": "에러 발생 페이지 소스"}
        response = requests.post(DISCORD_WEBHOOK, data=payload, files=files)

    # 전송 결과 확인
    print(f"response code : {response.status_code}")
    print(response.text)


class PAGES:
    """각 Page들의 SuperClass"""

    def __init__(self):
        self.refresh_delay = 30  # sec

    def pub_item_links(self, message):
        """SQS로 Crawl 정보 Publish"""
        sqs = boto3.client("sqs", region_name=REGION)
        queue_url = QUEUE_URL
        message_body = json.dumps(message)
        crawled_site = self.__class__.__name__
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageAttributes={
                "is_crawling": {"DataType": "String", "StringValue": "1"},
                "site_name": {"DataType": "String", "StringValue": crawled_site},
            },
        )


class QUASAR_ZONE(PAGES):
    def __init__(self):
        super().__init__()
        self.site_name = QUASAR_ZONE_LINK
        self.selectors = CRAWLER_SELECTORS["QUASAR_ZONE"]["crawling_selectors"]

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    comment,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name_text = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["item_name_css"]
                ).text
                # Assuming item_name needs specific processing like .split()[2:]
                item_name = (
                    " ".join(item_name_text.split()[2:]) if item_name_text else "err"
                )
                created_at = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["created_at_css"]
                ).text
                content = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["content_css"]
                ).text
                comment_elements = driver.find_elements(
                    By.CSS_SELECTOR, self.selectors["comment_list_css"]
                )
                comment = list(map(lambda x: x.text, comment_elements))
                category = driver.find_element(
                    By.XPATH, self.selectors["category_xpath"]
                ).text
                shopping_mall_link = driver.find_element(
                    By.XPATH, self.selectors["shopping_mall_link_xpath"]
                ).text
                shopping_mall = driver.find_element(
                    By.XPATH, self.selectors["shopping_mall_xpath"]
                ).text
                price = driver.find_element(
                    By.XPATH, self.selectors["price_xpath"]
                ).text
                delivery = driver.find_element(
                    By.XPATH, self.selectors["delivery_xpath"]
                ).text
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                # Consider adding capture_and_send_screenshot(driver, self.__class__.__name__) here if needed

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


class ARCA_LIVE(PAGES):
    def __init__(self):
        super().__init__()
        self.site_name = ARCA_LIVE_LINK
        self.selectors = CRAWLER_SELECTORS["ARCA_LIVE"]["crawling_selectors"]

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    comment,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                table = driver.find_element(
                    By.TAG_NAME, self.selectors["table_tag_name"]
                )
                rows = table.find_elements(By.TAG_NAME, self.selectors["row_tag_name"])
                details = [row.text for row in rows]
                # This mapping logic remains highly dependent on the structure and order of text in rows
                shopping_mall_link, shopping_mall, item_name, price, delivery = list(
                    map(lambda x: "".join(x.split()[1:]), details)
                )
                content = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["content_css"]
                ).text
                comment_box = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["comment_box_css"]
                )
                comment_elements = comment_box.find_elements(
                    By.CLASS_NAME, self.selectors["comment_text_class_name"]
                )
                comment = list(map(lambda x: x.text, comment_elements))
                created_at = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["created_at_css"]
                ).text
                category = driver.find_element(
                    By.XPATH, self.selectors["category_xpath"]
                ).text
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                # Consider adding capture_and_send_screenshot(driver, self.__class__.__name__) here if needed

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


class RULI_WEB(PAGES):
    def __init__(self):
        super().__init__()
        self.site_name = RULI_WEB_LINK
        self.selectors = CRAWLER_SELECTORS["RULI_WEB"]["crawling_selectors"]

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    comment,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["item_name_css"]
                ).text
                # shopping_mall extraction logic remains dependent on item_name format
                shopping_mall_match = re.findall(r"\[.+\]", item_name)
                if shopping_mall_match:
                    shopping_mall = shopping_mall_match[0]
                created_at = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["created_at_css"]
                ).text
                content = driver.find_element(
                    By.TAG_NAME, self.selectors["content_tag_name"]
                ).text
                comment_elements = driver.find_elements(
                    By.CLASS_NAME, self.selectors["comment_class_name"]
                )
                comment = list(map(lambda x: x.text, comment_elements))
                shopping_mall_link = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["shopping_mall_link_css"]
                ).text
                category = driver.find_element(
                    By.XPATH, self.selectors["category_xpath"]
                ).text

            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                # Consider adding capture_and_send_screenshot(driver, self.__class__.__name__) here if needed

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
        super().__init__()
        self.site_name = FM_KOREA_LINK
        self.selectors = CRAWLER_SELECTORS["FM_KOREA"]["crawling_selectors"]

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)
            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    comment,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                # The logic for unpacking 'details' is highly dependent on the number and order of elements with class "xe_content"
                details_elements = driver.find_elements(
                    By.CLASS_NAME, self.selectors["details_class_name"]
                )
                # Ensure robust unpacking if the number of elements can vary
                if len(details_elements) >= 6:
                    (
                        shopping_mall_link_el,
                        shopping_mall_el,
                        item_name_el,
                        price_el,
                        delivery_el,
                        content_el,
                    ) = details_elements[:6]
                    comment_elements = details_elements[6:]

                    shopping_mall_link = shopping_mall_link_el.text
                    shopping_mall = shopping_mall_el.text
                    item_name = item_name_el.text
                    price = price_el.text
                    delivery = delivery_el.text
                    content = content_el.text
                    comment = list(map(lambda x: x.text, comment_elements))
                else:
                    # Handle cases where not enough elements are found
                    print(
                        f"Warning: Not enough 'xe_content' elements found for {item_link}"
                    )

                created_at = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["created_at_css"]
                ).text
                category = driver.find_element(
                    By.XPATH, self.selectors["category_xpath"]
                ).text
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                # Consider adding capture_and_send_screenshot(driver, self.__class__.__name__) here if needed

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


class PPOM_PPU(PAGES):
    def __init__(self):
        super().__init__()
        self.site_name = PPOM_PPU_LINK
        self.selectors = CRAWLER_SELECTORS["PPOM_PPU"]["crawling_selectors"]

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)

            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    comment,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name = driver.find_element(
                    By.CSS_SELECTOR, self.selectors["item_name_css"]
                ).text
                content = driver.find_element(
                    By.XPATH, self.selectors["content_xpath"]
                ).text
                comment = driver.find_element(By.ID, self.selectors["comment_id"]).text
                created_at = driver.find_element(
                    By.XPATH, self.selectors["created_at_xpath"]
                ).text.lstrip("등록일 ")
                shopping_mall_link = driver.find_element(
                    By.XPATH, self.selectors["shopping_mall_link_xpath"]
                ).text
                shopping_mall = driver.find_element(
                    By.XPATH, self.selectors["shopping_mall_xpath"]
                ).text

            except Exception as e:
                # major한 shopping_mall이 아니면 path가 달라짐
                if item_name != "err" and shopping_mall == "err":
                    shopping_mall_match = re.match(
                        r"\[.+\]", item_name
                    )  # Use re.match for safety
                    if shopping_mall_match:
                        shopping_mall = shopping_mall_match[0]
                print(f"fail get item link {item_link} {str(e)}")
                # Consider adding capture_and_send_screenshot(driver, self.__class__.__name__) here if needed

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


class COOL_ENJOY(PAGES):
    def __init__(self):
        super().__init__()
        self.site_name = COOL_ENJOY_LINK
        self.selectors = CRAWLER_SELECTORS["COOL_ENJOY"]["crawling_selectors"]

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)

            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    comment,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                title_text = driver.find_element(
                    By.XPATH, self.selectors["title_text_xpath"]
                ).text
                # Logic for splitting title_text remains
                split_idx = title_text.find("|")
                if (
                    split_idx != -1 and split_idx > 3
                ):  # Ensure '|' is found and there's space for "-3"
                    category = title_text[: split_idx - 3].strip()
                    item_name = title_text[split_idx + 1 :].strip()
                else:  # Fallback if pattern doesn't match
                    category = "err"
                    item_name = (
                        title_text  # or some other default based on expected format
                    )

                content = driver.find_element(
                    By.XPATH, self.selectors["content_xpath"]
                ).text
                comment = driver.find_element(By.ID, self.selectors["comment_id"]).text
                created_at = driver.find_element(
                    By.XPATH, self.selectors["created_at_xpath"]
                ).text
                shopping_mall_link = driver.find_element(
                    By.XPATH, self.selectors["shopping_mall_link_xpath"]
                ).text
                # shopping_mall extraction logic remains dependent on item_name format
                shopping_mall_candidate = re.search(r"\[([^]]+)\]", item_name)
                if shopping_mall_candidate:
                    shopping_mall = shopping_mall_candidate.group(1)
            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                # Consider adding capture_and_send_screenshot(driver, self.__class__.__name__) here if needed

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


class EOMI_SAE(PAGES):
    def __init__(self):
        super().__init__()
        self.site_name = EOMI_SAE_LINK
        self.selectors = CRAWLER_SELECTORS["EOMI_SAE"]["crawling_selectors"]

    def crawling(self, driver, item_link_list):
        for item_link in item_link_list:
            driver.get(item_link)

            try:
                (
                    created_at,
                    shopping_mall_link,
                    shopping_mall,
                    price,
                    item_name,
                    delivery,
                    content,
                    comment,
                    category,
                ) = "err", "err", "err", "err", "err", "err", "err", "err", "err"
                item_name = driver.find_element(
                    By.XPATH, self.selectors["item_name_xpath"]
                ).text
                category = driver.find_element(
                    By.XPATH, self.selectors["category_xpath"]
                ).text
                content = driver.find_element(
                    By.XPATH, self.selectors["content_xpath"]
                ).text
                comment = driver.find_element(
                    By.CLASS_NAME, self.selectors["comment_class_name"]
                ).text
                created_at = driver.find_element(
                    By.XPATH, self.selectors["created_at_xpath"]
                ).text
                shopping_mall_link = driver.find_element(
                    By.XPATH, self.selectors["shopping_mall_link_xpath"]
                ).get_attribute("href")

            except Exception as e:
                print(f"fail get item link {item_link} {str(e)}")
                # Consider adding capture_and_send_screenshot(driver, self.__class__.__name__) here if needed

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


def set_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = "/opt/chrome/chrome"
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.add_argument("window-size=1392x1150")
    chrome_options.add_argument("disable-gpu")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    chrome_options.add_argument("--incognito")
    service = Service(executable_path="/opt/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    stealth(
        driver,
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

        message = event["Records"][0]["Sns"]["MessageAttributes"]
        site_name = message["site_name"]["Value"]
        item_link_list = ast.literal_eval(event["Records"][0]["Sns"]["Message"])

        # 크롤링 클래스 선택
        crawler_class = globals()[site_name]
        crawler = crawler_class()

        crawler.crawling(driver, item_link_list)

        driver.quit()
        return {
            "statusCode": 200,
            "body": json.dumps("Crawling completed successfully"),
        }

    except Exception as e:
        print(f"Error in handler: {str(e)}")
        if "driver" in locals():
            driver.quit()
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error during crawling: {str(e)}"),
        }


if __name__ == "__main__":
    handler(event, context)
