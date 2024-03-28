from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import Keys, ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.options import Options
import requests
import re
import random
import time
import concurrent.futures
from datetime import datetime


class PAGES:
    def __init__():
        pass
    
    def set_drvier(self, site_name):
        service = Service(executable_path=ChromeDriverManager().install())
        chrome_options = Options()
        # chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options = chrome_options, service=service)
        driver.implicitly_wait(10)
        driver.get(site_name)

        return driver
    
class ARCA_LIVE(PAGES):
    def __init__(self):
        self.hot_deal_page = "https://arca.live/b/hotdeal"
    
    def crawling(self):
        driver = self.set_drvier(self.hot_deal_page)

        item_names = []
        item_links = []
        shopping_mall_links = []
        shopping_malls = []
        prices = []
        deliveries = []
        contents = []
        comments = []

        for i in range(4, 49): # hot deal 게시판에서 item을 하나씩 가져옴
            item = driver.find_element(By.CSS_SELECTOR, f"body > div.root-container > div.content-wrapper.clearfix > article > div > div.article-list > div.list-table.hybrid > div:nth-child({i}) > div > div > span.vcol.col-title > a")
            item_links.append(item.get_attribute("href")) 

        for i in item_links:
            driver.get(i)
            table = driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.TAG_NAME, "tr")
            details = [row.text for row in rows]
            shopping_mall_link, shopping_mall, item_name, price, delivery = list(map(lambda x: "".join(x.split()[1:]), details))
            content = driver.find_element(By.CSS_SELECTOR, "body > div.root-container > div.content-wrapper.clearfix > article > div > div.article-wrapper > div.article-body > div.fr-view.article-content")
            comment_box = driver.find_element(By.CSS_SELECTOR, "#comment > div.list-area")
            comment = comment_box.find_elements(By.CLASS_NAME, "text")

            shopping_mall_links.append(shopping_mall_link)
            shopping_malls.append(shopping_mall)
            item_names.append(item_name)
            prices.append(price)
            deliveries.append(delivery)
            contents.append(content.text)
            comments.append(list(map(lambda x: x.text, comment)))   
            
class RULI_WEB(PAGES):
    def __init__(self):
        self.hot_deal_page = "https://bbs.ruliweb.com/market/board/1020?view=gallery"
        
    def crawling(self):
        driver = self.set_drvier(self.hot_deal_page)

        item_names = []
        item_links = []
        shopping_mall_links = []
        shopping_malls = []
        contents = []
        comments = []

        for i in range(1, 29): # hot deal 게시판에서 item을 하나씩 가져옴
            item = driver.find_element(By.CSS_SELECTOR, f"#board_list > div > div.board_main.theme_default.theme_white.theme_white.theme_gallery > table > tbody > tr:nth-child(7) > td > div > div:nth-child({i}) > div > div.article_info > div > div > a.deco")
            item_links.append(item.get_attribute("href").rstrip("?")) 
            shopping_mall = re.match("\[[^\[\]]+\]", item.text)
            if not shopping_mall: # shopping_mall 태그가 없는 경우
                shopping_malls.append("")
            else:
                shopping_malls.append(shopping_mall[0][1:-1])
                
        for i in item_links:
            driver.get(i)
            try:
                item_name = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_top > div.user_view > div:nth-child(1) > h4 > span > span.subject_inner_text")
                shopping_mall_link = driver.find_element(By.CSS_SELECTOR, "#board_read > div > div.board_main > div.board_main_view > div.source_url > a")
                content = driver.find_element(By.TAG_NAME, "article")
                comment = driver.find_elements(By.CLASS_NAME, "comment")
            except Exception as e:
                # print(e)
                item_names.append("")
                shopping_mall_links.append("")
                contents.append("")
                comments.append([])
                continue
            
            item_names.append(item_name.text)
            shopping_mall_links.append(shopping_mall_link.text)
            contents.append(content.text)
            comments.append(list(map(lambda x: x.text, comment)))
        
class FM_KOREA(PAGES):
    def __init__(self):
        self.hot_deal_page = "https://www.fmkorea.com/hotdeal"
        
    def crawling(self):
        driver = self.set_drvier(self.hot_deal_page)
        
        item_names = []
        item_links = []
        shopping_mall_links = []
        shopping_malls = []
        prices = []
        deliveries = []
        contents = []
        comments = []
        
        for i in range(1, 21): # hot deal 게시판에서 item을 하나씩 가져옴
            item = driver.find_element(By.CSS_SELECTOR, f"#bd_1196365581_0 > div > div.fm_best_widget._bd_pc > ul > li:nth-child({i}) > div > h3 > a")
            item_links.append(item.get_attribute("href"))
        
        for i in item_links:
            driver.get(i)
            details = driver.find_elements(By.CLASS_NAME, "xe_content")
            shopping_mall_link, shopping_mall, item_name, price, delivery, content, *comment = details
            shopping_mall_links.append(shopping_mall_link.text)
            shopping_malls.append(shopping_mall.text)
            item_names.append(item_name)
            prices.append(price.text)
            deliveries.append(delivery.text)
            contents.append(content.text)
            comments.append(list(map(lambda x: x.text, comment)))
            
class QUASAR_ZONE(PAGES):
    def __init__(self):
        self.hot_deal_page = "https://quasarzone.com/bbs/qb_saleinfo"
        
    def crawling(self):
        driver = self.set_drvier(self.hot_deal_page)
        # TBD
        
class PPOM_PPU(PAGES):
    def __init__(self):
        self.hot_deal_page ="https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu"
        
    def crawling(self):
        driver = self.set_drvier(self.hot_deal_page)
        # TBD
        
arca_live = ARCA_LIVE()
ruli_web = RULI_WEB()
fm_korea = FM_KOREA()
quarsar_zone = QUASAR_ZONE()
ppomppu = PPOM_PPU()