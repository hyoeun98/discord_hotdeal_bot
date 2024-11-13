
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

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

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

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")


consumer = KafkaConsumer(
    'test', # 토픽명
    bootstrap_servers=['localhost:29092', 'localhost:39092', 'localhost:49092'], # 카프카 브로커 주소 리스트
    auto_offset_reset='earliest', # 오프셋 위치(earliest:가장 처음, latest: 가장 최근)
    enable_auto_commit=True, # 오프셋 자동 커밋 여부
    group_id = "discord_bot",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')), # 메시지의 값 역직렬화,
    key_deserializer=lambda x: json.loads(x.decode('utf-8')), # 키의 값 역직렬화
)

def set_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_options.add_argument('--block-new-web-contents')
    # chrome_options.add_argument('--window-size=1920x1080')
    # chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options = chrome_options)
    driver.implicitly_wait(5)
    return driver

class Crawler:
    def __init__(self):
        self.driver = set_driver()
            
    def consume_pages(self):
        for message in consumer:
            page = message.key
            item_link = message.value
            timestamp = datetime.fromtimestamp(message.timestamp / 1000)
            
            data = {
                "content" : message.value
            }
            data = json.dumps(data)
            response = requests.post(DISCORD_WEBHOOK, data = data, headers={"Content-Type": "application/json"})

            # 응답 확인
            if response.status_code == 204:
                print("Message sent successfully!")
            else:
                print(f"Failed to send message: {response.status_code}, {response.text}")
            
            
crawler = Crawler()
crawler.consume_pages()