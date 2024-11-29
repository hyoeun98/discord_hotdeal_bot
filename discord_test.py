
import pytz
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
from scanner import PAGES, SITES
import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import threading


load_dotenv()
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")


# Logging 설정
logging.basicConfig(filename='discord.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Discord bot 초기화
class ChannelManager:
    def __init__(self):
        self.init_db()

    def get_connection(self):
        """데이터베이스 연결 생성"""
        return psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
    def init_db(self):
        """데이터베이스 초기화"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # 채널 테이블 생성
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS channels (
                        channel_id BIGINT PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        channel_name TEXT NOT NULL,
                        guild_name TEXT NOT NULL,
                        category TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_message_at TIMESTAMP,
                        message_count INTEGER DEFAULT 0,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                ''')
                
                # 메시지 로그 테이블 생성
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS message_logs (
                        id SERIAL PRIMARY KEY,
                        channel_id BIGINT,
                        message TEXT NOT NULL,
                        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status TEXT,
                        error_message TEXT,
                        FOREIGN KEY (channel_id) REFERENCES channels (channel_id)
                    )
                ''')
                conn.commit()
        finally:
            conn.close()
            
    def add_channel(self, channel_id, guild_id, channel_name, guild_name, category=None):
        """채널 추가"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO channels 
                    (channel_id, guild_id, channel_name, guild_name, category)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (channel_id) 
                    DO UPDATE SET 
                        guild_id = EXCLUDED.guild_id,
                        channel_name = EXCLUDED.channel_name,
                        guild_name = EXCLUDED.guild_name,
                        category = EXCLUDED.category,
                        is_active = TRUE
                ''', (channel_id, guild_id, channel_name, guild_name, category))
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"채널 추가 중 오류 발생: {e}")
            return False
        finally:
            conn.close()

    def remove_channel(self, channel_id):
        """채널 제거 (비활성화)"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('''
                    UPDATE channels
                    SET is_active = FALSE
                    WHERE channel_id = %s
                ''', (channel_id,))
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"채널 제거 중 오류 발생: {e}")
            return False
        finally:
            conn.close()

    def get_active_channels(self):
        """활성화된 채널 목록 조회"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT channel_id FROM channels WHERE is_active = TRUE')
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def log_message(self, channel_id, message, status, error_message=None):
        """메시지 전송 로그 기록"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO message_logs (channel_id, message, status, error_message)
                    VALUES (%s, %s, %s, %s)
                ''', (channel_id, message, status, error_message))
                
                if status == 'success':
                    cur.execute('''
                        UPDATE channels
                        SET message_count = message_count + 1,
                            last_message_at = CURRENT_TIMESTAMP
                        WHERE channel_id = %s
                    ''', (channel_id,))
                conn.commit()
        except Exception as e:
            logging.error(f"메시지 로깅 중 오류 발생: {e}")
        finally:
            conn.close()

    def get_channel_stats(self, channel_id):
        """채널 통계 조회"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT channel_name, guild_name, message_count, last_message_at
                    FROM channels
                    WHERE channel_id = %s
                ''', (channel_id,))
                return cur.fetchone()
        finally:
            conn.close()
            
    def add_keyword(self, channel_id, keyword):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT keyword FROM channel_keyword WHERE channel_id = %s AND keyword = %s", (channel_id, keyword))
                exist = cur.fetchone()
                
                if exist:
                    return f"{keyword}는 이미 등록된 키워드입니다."
                else:
                    cur.execute("INSERT INTO channel_keyword (channel_id, keyword) VALUES (%s, %s)", (channel_id, keyword))
                    conn.commit()
                    return f"{keyword} 키워드 등록 완료."
                
        except Exception as e:
            logging.error(f"키워드 등록 중 오류 발생: {e}")
            return (f"키워드 등록 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()
            
    def del_keyword(self, channel_id, keyword):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT keyword FROM channel_keyword WHERE channel_id = %s AND keyword = %s", (channel_id, keyword))
                exist = cur.fetchone()
                
                if not exist:
                    return f"{keyword}는 등록되지 않은 키워드입니다."
                else:
                    cur.execute("DELETE FROM channel_keyword WHERE channel_id = %s AND keyword = %s", (channel_id, keyword))
                    conn.commit()
                    return f"{keyword} 키워드 삭제 완료."
                
        except Exception as e:
            logging.error(f"키워드 삭제 중 오류 발생: {e}")
            return (f"키워드 삭제 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()
    
    def get_keyword(self, channel_id):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT keyword FROM channel_keyword WHERE channel_id = %s", (channel_id,))
                keywords = cur.fetchall()
                if not keywords:
                    return f"등록된 키워드가 없습니다."
                else:
                    keyword_list = ', '.join(keyword[0] for keyword in keywords)
                    return f"등록된 키워드 : {keyword_list}"
                
        except Exception as e:
            logging.error(f"키워드 조회 중 오류 발생: {e}")
            return (f"키워드 조회 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()
            
# Discord bot 초기화
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
bot = commands.Bot(command_prefix='/', intents=intents)
channel_manager = ChannelManager()

@bot.event
async def on_ready():
    logging.info(f'{bot.user} 로 로그인했습니다!')
    # Kafka consumer 시작
    threading.Thread(target=run_kafka_consumer, daemon=True).start()
    
@bot.event
async def on_guild_join(guild):
    # 봇이 새로운 서버에 초대되었을 때
    channel = guild.system_channel  # 시스템 채널을 가져옵니다.
    
    # 시스템 채널이 존재하는 경우 안내 메시지를 보냅니다.
    if channel is not None:
        await channel.send(
            f"- 등록 : /register
            - 해제 : /unregister"
        )

@bot.command()
async def add_keyword(ctx, *, keyword):
    """알람 keyword 등록"""
    result = channel_manager.add_keyword(ctx.channel.id, keyword)
    await ctx.send(result)

@bot.command()
async def del_keyword(ctx, *, keyword):
    """알람 keyword 삭제"""
    result = channel_manager.del_keyword(ctx.channel.id, keyword)
    await ctx.send(result)
    
@bot.command()
async def get_keyword(ctx):
    """알람 keyword 삭제"""
    result = channel_manager.get_keyword(ctx.channel.id)
    await ctx.send(result)
    
@bot.command(name="register", description="이 채널에 핫딜정보를 출력합니다.")
async def register(ctx):
    """현재 채널을 등록"""
    channel = ctx.channel
    category_name = channel.category.name if channel.category else None
    success = channel_manager.add_channel(
        channel.id, 
        ctx.guild.id,
        channel.name,
        ctx.guild.name,
        category_name
    )
    if success:
        await ctx.send(f'채널이 등록되었습니다: {channel.name}')
    else:
        await ctx.send('채널 등록에 실패했습니다.')
        
    threads = channel.threads
    thread_exists = any(thread.name == "keyword" for thread in threads)
    
    if not thread_exists:
        new_thread = await channel.create_thread(
            name = "keyword",
            auto_archive_duration = 0
        )
        await new_thread.send(f"keyword thread 생성")
    
    else:
        await ctx.send(f"keyword thread가 이미 존재합니다.")

@bot.command()
async def unregister(ctx):
    """현재 채널을 등록 해제"""
    success = channel_manager.remove_channel(ctx.channel.id)
    if success:
        await ctx.send(f'채널이 등록 해제되었습니다: {ctx.channel.name}')
    else:
        await ctx.send('채널 등록 해제에 실패했습니다.')

@bot.command()
async def stats(ctx):
    """채널 통계 조회"""
    stats = channel_manager.get_channel_stats(ctx.channel.id)
    if stats:
        channel_name, guild_name, msg_count, last_msg = stats
        await ctx.send(f'''
채널 통계:
- 채널명: {channel_name}
- 서버명: {guild_name}
- 전송된 메시지 수: {msg_count}
- 마지막 메시지 전송: {last_msg}
''')
    else:
        await ctx.send('채널 통계를 찾을 수 없습니다.')
        
def transform_message(message):
    content = message["content"]
    content = re.sub(r"\n+", "\n", content.strip())
    embed = discord.Embed(title=f"{message['item_name']}", description=f"{message['site']}", timestamp=datetime.now(pytz.timezone("UTC")))
    embed.add_field(name="원문 링크", value=message["item_link"], inline=True)
    embed.add_field(name="구매 링크", value=message["shopping_mall_link"], inline=True)
    embed.add_field(name="본문", value=content, inline=False)
    return embed

async def send_message_to_channels(message_content):
    """등록된 채널에 메시지 전송"""
    channels = channel_manager.get_active_channels()
    
    for channel_id in channels:
        channel = bot.get_channel(channel_id)
        if channel:
            try:
                embed = transform_message(message_content)
                await channel.send(embed=embed)
                channel_manager.log_message(channel_id, message_content, 'success')
                logging.info(f'메시지 전송 성공: {channel.guild.name}/{channel.name}')
            except Exception as e:
                error_msg = str(e)
                channel_manager.log_message(channel_id, message_content, 'failed', error_msg)
                logging.error(f'채널 {channel.guild.name}/{channel.name}에 메시지 전송 실패: {error_msg}')
                
def run_kafka_consumer():
    """Kafka consumer 실행"""
    logging.info("Kafka consumer 시작")
    consumer = KafkaConsumer(
        'transformed_message', # 토픽명
        bootstrap_servers=['localhost:29092', 'localhost:39092', 'localhost:49092'], # 카프카 브로커 주소 리스트
        auto_offset_reset='earliest', # 오프셋 위치(earliest:가장 처음, latest: 가장 최근)
        enable_auto_commit=True, # 오프셋 자동 커밋 여부
        group_id = "discord_bot_test",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')), # 메시지의 값 역직렬화,
        key_deserializer=lambda x: json.loads(x.decode('utf-8')), # 키의 값 역직렬화
    )
    
    for message in consumer:
        status = message.key
        discord_message = message.value
        logging.info(f"Kafka 메시지 수신: {discord_message}")
        if status == "success":
            asyncio.run_coroutine_threadsafe(
                send_message_to_channels(discord_message),
                bot.loop
            )

def main():
    """메인 함수"""
    bot.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()