
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
import re
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
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
from scanner import PAGES, SITES, save_full_screenshot

load_dotenv()

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

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

class Crawler:
    def __init__(self):
        self.driver = set_driver()
            
    def send_discord(self, **kwargs):
        data = {
            "content" :f"""page : {kwargs["page"]}
            item_link : {kwargs["item_link"]}
                        """
        }
        data = json.dumps(data)
        response = requests.post(DISCORD_WEBHOOK, data = data, headers={"Content-Type": "application/json"})

        # 응답 확인
        if response.status_code == 204:
            logging.info(f"Message insert, {kwargs['item_link']}")
        else:
            logging.error(f"Failed to send message: {response.status_code}, {response.text} {kwargs['item_link']}")
            
    def crawling_error_logging(self, e: Exception, error_type, item_link, **kwargs):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # 현재 시간을 포맷팅
        error_log = {"error_log": e, "time": timestamp, "error_type": error_type}
        screenshot_filename = f'error_screenshot/{self.__class__.__name__}_{timestamp}.png'
        save_full_screenshot(self.driver, screenshot_filename)
        
        if kwargs:
            for k, v in kwargs:
                error_log[k] = v
        
        try:
            if os.path.exists(screenshot_filename):
                with open(screenshot_filename, 'rb') as file:
                    response = crawler_client.files_upload_v2(
                        channel=SLACK_CHANNEL_ID,
                        file=file,
                        filename=os.path.basename(screenshot_filename),  # 파일 이름
                        initial_comment=error_log  # 업로드할 때 이미지 제목
                    )
                logging.info(f"File uploaded successfully: {response['file']['permalink']}")
        except SlackApiError as e:
            logging.error(f"Error uploading file: {e.response['error']}")
        
        try:
            crawler_cursor.execute(crawling_error_insert_query, (self.__class__.__name__, str(error_log), timestamp, item_link))
            crawler_connection.commit()
        except Exception as e:
            logging.info(e)
            crawler_connection.rollback()
    
    def consume_pages(self):
        for message in consumer:
            page = message.key
            item_link = message.value
            logging.info(item_link)
            # self.send_discord(page = page, item_link = item_link)
            if item_link is not None:
                self.crawling(page, item_link)
                time.sleep(1)
            else:
                logging.info("None123")
            
    def crawling(self, page, item_link):
        if page not in SITES:
            raise TypeError(f"Invalid page name : {page}")
        
        insert_table = page.lower()
        crawling_result_insert_query = sql.SQL(f"""
            INSERT INTO {insert_table} (created_at, item_name, item_link, shopping_mall, shopping_mall_link, price, delivery)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """)
        
        try:
            result = SITES[page].crawling(self.driver, item_link)
            result["site"] = page
            value = (result["created_at"], result["item_name"], item_link, result["shopping_mall"], result["shopping_mall_link"], result["price"], result["delivery"])
        except Exception as e:
            self.crawling_error_logging(e, f"fail crawling {item_link}", item_link)
            self.driver.close()
            self.driver = set_driver()
            
        try:
            crawler_cursor.execute(crawling_result_insert_query, value)
            crawler_connection.commit()
            logging.info(f"success insert {item_link}")
        except Exception as e:
            crawler_connection.rollback()
            self.crawling_error_logging(e, f"fail insert {item_link}", item_link)
        
#         transformed_message = f'''
# # {result["item_name"]}
# - [원본 링크]({item_link})
# - [구매 링크]({result["shopping_mall_link"]})
# ```{content}```
# -# {result["created_at"]} {page}
# '''
        try:
            producer.send(topic = 'transformed_message', value=result, key = "fail" if result["item_name"] == "err" else "success")
        except Exception as e:
            self.crawling_error_logging(e, f"fail publishing {item_link}", item_link)
        
if __name__ == "__main__":
    crawler_connection = psycopg2.connect(
    dbname = DB_NAME,
    user = DB_USER,
    password = DB_PASSWORD,
    host = DB_HOST,
    port = DB_PORT
    )
    crawler_cursor = crawler_connection.cursor()
    crawler_client = WebClient(token=SLACK_TOKEN)

    crawling_error_insert_query = sql.SQL("""
        INSERT INTO crawling_error (site_name_idx, error_log, timestamp, item_link)
        VALUES (%s, %s, %s, %s)
    """)

    consumer = KafkaConsumer(
        'test', # 토픽명
        bootstrap_servers=['localhost:29092'], # 카프카 브로커 주소 리스트
        auto_offset_reset='earliest', # 오프셋 위치(earliest:가장 처음, latest: 가장 최근)
        enable_auto_commit=True, # 오프셋 자동 커밋 여부
        group_id = "discord_bot",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')), # 메시지의 값 역직렬화,
        key_deserializer=lambda x: json.loads(x.decode('utf-8')), # 키의 값 역직렬화
    )
    
    producer = KafkaProducer(
        acks=0, # 메시지 전송 완료에 대한 체크
        compression_type='gzip', # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
        bootstrap_servers=['localhost:29092'], # 전달하고자 하는 카프카 브로커의 주소 리스트
        value_serializer=lambda x:json.dumps(x, default=str).encode('utf-8'), # 메시지의 값 직렬화
        key_serializer=lambda x:json.dumps(x, default=str).encode('utf-8') # 키의 값 직렬화
    )
    
    logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Start Crawling")
    
    crawler = Crawler()
    crawler.consume_pages()