from logging.handlers import RotatingFileHandler
import json
import logging
import re
from datetime import datetime
from dotenv import load_dotenv
import os
import discord
from discord.ext import commands
import asyncio
from collections import defaultdict
import psycopg2
from openai import AsyncOpenAI
import pendulum
import redis.asyncio as redis
from datetime import timedelta, timezone
import traceback
from contextlib import suppress

SITE_NAMES = [
    "QUASAR_ZONE",
    "PPOM_PPU",
    "FM_KOREA",
    "ARCA_LIVE",
    "COOL_ENJOY",
    "EOMI_SAE",
    "RULI_WEB",
]

class ChannelManager:
    def __init__(self, db_config):
        """Init ChannelManager"""
        self.db_config = db_config

    def get_connection(self):
        """데이터베이스 연결 생성"""
        return psycopg2.connect(**self.db_config)
            
    def add_channel(self, channel_id, guild_id, channel_name, guild_name):
        """채널 추가"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO channels 
                    (channel_id, guild_id, channel_name, guild_name)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (channel_id) 
                    DO UPDATE SET 
                        guild_id = EXCLUDED.guild_id,
                        channel_name = EXCLUDED.channel_name,
                        guild_name = EXCLUDED.guild_name,
                        is_active = TRUE
                ''', (channel_id, guild_id, channel_name, guild_name))
                conn.commit()
                logging.info(f"채널 추가 완료 {guild_name} / {channel_name} channel id:{channel_id}")
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
                logging.info(f"채널 삭제 완료 channel id:{channel_id}")
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

    def log_message(self, message_id, channel_id=None, embeds=None, guild_id=None, item_link=None):
        """메시지 로그 기록 (PostgreSQL message_logs 테이블에 저장)"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO message_logs (
                        channel_id,
                        message_id,
                        embeds,
                        created_at,
                        guild_id,
                        item_link
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                ''', (
                    channel_id,
                    message_id,
                    json.dumps(embeds) if embeds else None,
                    datetime.now(timezone.utc),
                    guild_id,
                    item_link
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"메시지 로그 기록 중 오류 발생: {e}")
        finally:
            conn.close()


    # def get_channel_stats(self, channel_id):
    #     """채널 통계 조회"""
    #     conn = self.get_connection()
    #     try:
    #         with conn.cursor() as cur:
    #             cur.execute('''
    #                 SELECT channel_name, guild_name, message_count, last_message_at
    #                 FROM channels
    #                 WHERE channel_id = %s
    #             ''', (channel_id,))
    #             return cur.fetchone()
    #     finally:
    #         conn.close()
            
            
    def get_keyword(self, channel_id, user_id):
        """keyword 출력"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT keyword FROM keywords WHERE channel_id = %s AND user_id = %s", (channel_id, user_id))
                keywords = cur.fetchall()
                if not keywords:
                    return "등록된 키워드가 없습니다."
                else:
                    keyword_list = ', '.join(keyword[0] for keyword in keywords)
                    return f"등록된 키워드 : {keyword_list}"
                
        except Exception as e:
            logging.error(f"키워드 조회 중 오류 발생: {e}")
            return (f"키워드 조회 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()
            
    def add_keyword(self, channel_id, user_id, keyword):
        """keyword 추가"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM threads WHERE channel_id = %s AND user_id = %s", (channel_id, user_id))
                exist = cur.fetchone()
                if not exist:
                    return "/make_keyword_thread 명령어를 실행해주세요!"
                
                cur.execute("SELECT keyword FROM keywords WHERE channel_id = %s AND user_id = %s AND keyword = %s", (channel_id, user_id, keyword))
                exist = cur.fetchone()
                
                if exist:
                    return f"{keyword}는 이미 등록된 키워드입니다."
                else:
                    cur.execute("INSERT INTO keywords (channel_id, user_id, keyword) VALUES (%s, %s, %s)", (channel_id, user_id, keyword))
                    conn.commit()
                    return f"{keyword} 키워드 등록 완료."
                
        except Exception as e:
            logging.error(f"키워드 등록 중 오류 발생: {e}")
            return (f"키워드 등록 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()
                        
    def del_keyword(self, channel_id, user_id, keyword):
        """keyword 삭제"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT keyword FROM keywords WHERE channel_id = %s AND user_id = %s AND keyword = %s", (channel_id, user_id, keyword))
                exist = cur.fetchone()
                
                if not exist:
                    return f"{keyword}는 등록되지 않은 키워드입니다."
                else:
                    cur.execute("DELETE FROM keywords WHERE channel_id = %s AND user_id = %s AND keyword = %s", (channel_id, user_id, keyword))
                    conn.commit()
                    return f"{keyword} 키워드 삭제 완료."
                
        except Exception as e:
            logging.error(f"키워드 삭제 중 오류 발생: {e}")
            return (f"키워드 삭제 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()    
    
    def map_keyword_to_channel(self, message):
        """message에 keyword가 있는지 확인하고
        dict[channel_id] = [(user_id, keyword), ...]로 return"""
        keywords = self.get_keyword_channel_user()
        channel_keyword_mapping_dict = defaultdict(list)
        try:           
            for kw, channel_id, user_id in keywords:
                if kw.upper() in message["item_name"].upper() or kw.upper() in message["content"].upper() or kw.upper() in message["category"].upper() or kw.upper() in message["pred_category"].upper():  # 제목, 내용, 태그에 keyword가 있을 때 추가
                    channel_keyword_mapping_dict[channel_id].append((user_id, kw))
                            
            return channel_keyword_mapping_dict
        
        except Exception as e:
            logging.error(f"키워드 등록 유저조회 중 오류 발생: {e}")
            return []
            
    def get_keyword_channel_user(self):
        """모든 keyword 추출
        todo : 활성화된 channel의 keyword만 가져오기"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT keyword, channel_id, user_id FROM keywords")
                keywords = cur.fetchall()
                return keywords
                
        except Exception as e:
            logging.error(f"키워드 조회 중 오류 발생: {e}")
            return []
        
        finally:
            conn.close()
            
    def get_thread_channel_user(self):
        """모든 thread 추출"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT thread_id, channel_id, user_id FROM threads WHERE is_active = TRUE")
                threads = cur.fetchall()
                return threads
                
        except Exception as e:
            logging.error(f"스레드 조회 중 오류 발생: {e}")
            return []
        
        finally:
            conn.close()

    def get_trend_thread_channel_user(self):
        """모든 trend_thread 추출"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT thread_id, channel_id, user_id FROM trend_threads WHERE is_active = TRUE")
                threads = cur.fetchall()
                return threads
                
        except Exception as e:
            logging.error(f"trend 스레드 조회 중 오류 발생: {e}")
            return []
        
        finally:
            conn.close()
            
    def map_thread_to_user_and_keyword(self, body):
        """dict[thread_id] = [(user_id, keyword), ...]"""
        threads = self.get_thread_channel_user()
        channel_keyword_mapping_dict = self.map_keyword_to_channel(body)
        thread_user_and_keyword_mapping_dict = defaultdict(list)
        try:
            # (channel_id, user_id) -> thread_id 리스트로 매핑
            threads_by_channel_and_user = defaultdict(list)
            for thread_id, channel_id, user_id in threads:
                threads_by_channel_and_user[(channel_id, user_id)].append(thread_id)

            # 매핑된 dict로 lookup해서 append
            for channel_id, value in channel_keyword_mapping_dict.items():
                for user_id, kw in value:
                    thread_ids = threads_by_channel_and_user.get((channel_id, user_id), [])
                    for thread_id in thread_ids:
                        thread_user_and_keyword_mapping_dict[thread_id].append((user_id, kw))

            return thread_user_and_keyword_mapping_dict

        except Exception as e:
            logging.error(f"스레드 등록 유저조회 중 오류 발생: {e}")
            return []
            
    def save_message(self, message, site_name):
        """메시지를 DB에 저장"""
        conn = self.get_connection()
        try:
            created_at = message.get("created_at")

            # DB 저장은 UTC로 통일
            if isinstance(created_at, pendulum.DateTime):
                created_at_for_db = created_at.in_timezone("UTC")
            elif isinstance(created_at, datetime) and created_at.tzinfo is not None:
                created_at_for_db = created_at.astimezone(timezone.utc)
            elif isinstance(created_at, datetime):
                # naive면 KST로 들어왔다고 가정 후 UTC 변환
                kst = timezone(timedelta(hours=9))
                created_at_for_db = created_at.replace(tzinfo=kst).astimezone(timezone.utc)
            else:
                created_at_for_db = datetime.now(timezone.utc)

            with conn.cursor() as cur:
                cur.execute(f'''
                    INSERT INTO {site_name.lower()}
                    (item_name, item_link, shopping_mall_link, shopping_mall, delivery, price, created_at, category, pred_category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                ''', (
                    message["item_name"],
                    message["item_link"],
                    message["shopping_mall_link"],
                    message["shopping_mall"],
                    message["delivery"],
                    message["price"],
                    created_at_for_db,
                    message["category"],
                    message["pred_category"]
                ))
                message_id = cur.fetchone()[0]
                conn.commit()
                logging.info(f"새 메시지 저장 완료: {message_id}")
                return message_id

        except Exception as e:
            conn.rollback()
            logging.error(f"메시지 저장 중 오류 발생: {e}")
            return None

        finally:
            conn.close()



    def make_thread(self, channel_id, user_id, thread_id, thread_name, table_name):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT thread_name FROM threads WHERE channel_id = %s AND user_id = %s AND thread_id = %s AND thread_name = %s", (channel_id, user_id, thread_id, thread_name))
                exist = cur.fetchone()
                
                if exist:
                    return f"{thread_name}는 이미 등록된 스레드입니다."
                else:
                    query = f"INSERT INTO {table_name} (channel_id, user_id, thread_id, thread_name) VALUES (%s, %s, %s, %s)"
                    cur.execute(query, (channel_id, user_id, thread_id, thread_name))
                    conn.commit()
                    return f"{thread_name} 스레드 등록 완료."
                
        except Exception as e:
            logging.error(f"스레드 등록 중 오류 발생: {e}")
            return (f"스레드 등록 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()
            
    def del_thread(self, thread_id):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM threads WHERE thread_id = %s", (thread_id,))
                exist = cur.fetchone()
                
                if exist:  # threads 테이블일 시
                    cur.execute("DELETE FROM threads WHERE thread_id = %s", (thread_id,))
                    conn.commit()
                    return "스레드 삭제 완료."
                else:  # trend_threads 테이블일 시
                    cur.execute("SELECT id FROM trend_threads WHERE thread_id = %s", (thread_id,))
                    exist = cur.fetchone()

                    if exist:
                        cur.execute("DELETE FROM trend_threads WHERE thread_id = %s", (thread_id,))
                        conn.commit()
                        return "스레드 삭제 완료."
                    else:
                        return "등록되지 않은 스레드입니다."
                
        except Exception as e:
            logging.error(f"스레드 삭제 중 오류 발생: {e}")
            return (f"스레드 삭제 중 오류가 발생했습니다: {e}")
        
        finally:
            conn.close()
            
class HotDealBot:
    def __init__(self):
        self.db_config = {}
        self.bot = None
        self.sqs = None
        self.channel_manager = None
        self._background_tasks = []
        
    def setup_logging(self):
        """로깅 설정"""
        log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # record.created(에폭 초)를 KST로 변환해서 출력
        KST = timezone(timedelta(hours=9))
        log_formatter.converter = lambda secs: datetime.fromtimestamp(secs, tz=KST).timetuple()

        file_handler = RotatingFileHandler(
            '/home/hyoeun/hotdeal_bot/bot/logs/discord.log',
            maxBytes=3 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(log_formatter)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
            logger.addHandler(file_handler)

        logging.info("Logging setup completed")

    def setup_environment(self):
        """환경 변수 설정"""
        load_dotenv()
        
        # 필수 환경 변수 검증
        required_env_vars = [
            "DISCORD_TOKEN",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
            "DB_HOST",
            "DB_PORT",
            "OPENAI_API_KEY"  # OpenAI API 키 추가
        ]
        
        missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # 데이터베이스 설정
        self.db_config = {
            "dbname": os.environ.get("DB_NAME"),
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("DB_PASSWORD"),
            "host": os.environ.get("DB_HOST"),
            "port": os.environ.get("DB_PORT"),
            "options": "-c timezone=UTC",
        }
        self.redis_host = os.environ.get("REDIS_HOST", "localhost")
        self.redis_port = int(os.environ.get("REDIS_PORT", 6379))
        self.redis_db = int(os.environ.get("REDIS_DB", 0))
        self.redis_password = os.environ.get("REDIS_PASSWORD")
        self.crawl_channels = [f"crawl:{site_name}" for site_name in SITE_NAMES]
        self.trend_channels = [f"trend:{site_name}" for site_name in SITE_NAMES]
        
        # OpenAI 클라이언트 설정
        self.openai_client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        logging.info("Environment setup completed")

    def setup_bot(self):
        """Discord 봇 초기화"""
        parent = self

        class ManagedBot(commands.Bot):
            async def setup_hook(self):
                await parent.start_background_tasks()

            async def close(self):
                await parent.stop_background_tasks()
                await super().close()

        # Discord 봇 설정
        intents = discord.Intents.all()  # 모든 인텐트 활성화
        self.bot = ManagedBot(command_prefix='/', intents=intents)
        self.bot.remove_command("help")
        
        # ChannelManager 초기화
        self.channel_manager = ChannelManager(self.db_config)
        logging.info("Bot setup completed")
        
        @self.bot.event
        async def on_ready():
            logging.info(f'{self.bot.user} 로그인했습니다!')
            
            # 슬래시 명령어 동기화
            try:
                await self.bot.tree.sync()
                logging.info("슬래시 명령어 동기화 완료")
                
            except Exception as e:
                logging.error(f"명령어 동기화 실패: {e}")
                
            # 봇 상태 설정
            await self.bot.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.playing,
                    name="/help | 핫딜 정보 수집"
                )
            )
        
        @self.bot.event
        async def on_guild_join(guild):
            """봇이 새로운 서버에 초대되었을 때"""
            channel = guild.system_channel  # 시스템 채널을 가져옵니다.
            
            # 시스템 채널이 존재하는 경우 안내 메시지를 보냅니다.
            if channel is not None:
                await channel.send(
                    "안녕하세요! 명령어 목록을 보시려면 `/help`를 입력해주세요."
                )
        ##### Test method
        # @self.bot.tree.command(name="hello", description="봇이 인사를 합니다.")
        # async def hello(interaction: discord.Interaction):
        #     """간단한 인사 명령어"""
        #     await interaction.response.send_message(f"안녕하세요, {interaction.user.mention}님! 👋")
            
        # @self.bot.command(name="register", description="이 채널에 핫딜정보를 출력합니다.")
        # async def register(ctx):
        #     """현재 채널을 등록"""
        #     channel = ctx.channel
        #     active_channels = self.channel_manager.get_active_channels()
        #     if channel.id in active_channels:
        #         await ctx.send(f'이미 등록된 채널입니다: {channel.name}')
        #         return

        #     success = self.channel_manager.add_channel(
        #         channel.id,
        #         ctx.guild.id,
        #         channel.name,
        #         ctx.guild.name,
        #     )
        #     if success:
        #         await ctx.send(f'채널이 등록되었습니다: {channel.name}')
        #     else:
        #         await ctx.send('채널 등록에 실패했습니다.')
        
        @self.bot.tree.command(name="register", description="이 채널에 핫딜정보를 출력합니다.")
        async def register(interaction: discord.Interaction):
            """현재 채널을 등록"""
            channel = interaction.channel
            active_channels = self.channel_manager.get_active_channels()
            if channel.id in active_channels:
                await interaction.response.send_message(f'이미 등록된 채널입니다: {channel.name}')
                return

            success = self.channel_manager.add_channel(
                channel.id,
                interaction.guild.id,
                channel.name,
                interaction.guild.name,
            )
            if success:
                await interaction.response.send_message(f'채널이 등록되었습니다: {channel.name}')
            else:
                await interaction.response.send_message('채널 등록에 실패했습니다.')


        # @self.bot.command(name="unregister", description="이 채널의 핫딜정보 출력을 중단합니다.")
        # async def unregister(ctx):
        #     """현재 채널을 등록 해제"""
        #     channel_id = ctx.channel.id
        #     success = self.channel_manager.remove_channel(channel_id)
        #     if success:
        #         await ctx.send(f'채널이 해제되었습니다: {ctx.channel.name}')
        #     else:
        #         await ctx.send('채널 해제에 실패했습니다.')
        
        @self.bot.tree.command(name="unregister", description="이 채널의 핫딜정보 출력을 중단합니다.")
        async def unregister(interaction: discord.Interaction):
            """현재 채널을 등록 해제"""
            channel_id = interaction.channel.id
            success = self.channel_manager.remove_channel(channel_id)
            if success:
                await interaction.response.send_message(f'채널이 해제되었습니다: {interaction.channel.name}')
            else:
                await interaction.response.send_message('채널 해제에 실패했습니다.')

            
        # @self.bot.command(name="add_keyword", description="키워드를 등록하면 해당 키워드가 포함된 핫딜 발견 시 멘션을 보냅니다.")
        # async def add_keyword(ctx, *, keyword):
        #     """알람 keyword 등록"""
        #     channel_id = ctx.channel.id if isinstance(ctx.channel, discord.TextChannel) else ctx.channel.parent_id
        #     result = self.channel_manager.add_keyword(channel_id, ctx.author.id, keyword.strip())
        #     await ctx.send(result)
        
        # @self.bot.command(name="del_keyword", description="등록된 키워드를 삭제합니다.")
        # async def del_keyword(ctx, *, keyword):
        #     """알람 keyword 삭제"""
        #     channel_id = ctx.channel.id if isinstance(ctx.channel, discord.TextChannel) else ctx.channel.parent_id
        #     result = self.channel_manager.del_keyword(channel_id, ctx.author.id, keyword)
        #     await ctx.send(result)
        
        # @self.bot.command(name="get_keyword", description="현재 등록된 키워드 목록을 보여줍니다.")
        # async def get_keyword(ctx):
        #     """등록된 keyword 가져오기"""
        #     channel_id = ctx.channel.id if isinstance(ctx.channel, discord.TextChannel) else ctx.channel.parent_id
        #     result = self.channel_manager.get_keyword(channel_id, ctx.author.id)
        #     await ctx.send(result)

        @self.bot.tree.command(name="add_keyword", description="키워드를 등록하면 해당 키워드가 포함된 핫딜 발견 시 멘션을 보냅니다.")
        async def add_keyword(interaction: discord.Interaction, keyword: str):
            """알람 keyword 등록"""
            channel_id = interaction.channel.id if isinstance(interaction.channel, discord.TextChannel) else interaction.channel.parent_id
            result = self.channel_manager.add_keyword(channel_id, interaction.user.id, keyword.strip())
            # await interaction.response.send_message(result)
            
            keywords = self.channel_manager.get_keyword(channel_id, interaction.user.id)
            await interaction.response.send_message(f"`{keyword}` 등록 완료\n{keywords}")
            
        @self.bot.tree.command(name="del_keyword", description="등록된 키워드를 삭제합니다.")
        async def del_keyword(interaction: discord.Interaction, keyword: str):
            """알람 keyword 삭제"""
            channel_id = interaction.channel.id if isinstance(interaction.channel, discord.TextChannel) else interaction.channel.parent_id
            result = self.channel_manager.del_keyword(channel_id, interaction.user.id, keyword)

            if "등록되지 않은 키워드" in result:
                keywords = self.channel_manager.get_keyword(channel_id, interaction.user.id)
                await interaction.response.send_message(f"등록되지 않은 키워드: `{keyword}`\n{keywords}")
                return

            keywords = self.channel_manager.get_keyword(channel_id, interaction.user.id)
            await interaction.response.send_message(f"`{keyword}` 삭제 완료\n{keywords}")
            
        @self.bot.tree.command(name="get_keyword", description="현재 등록된 키워드 목록을 보여줍니다.")
        async def get_keyword(interaction: discord.Interaction):
            """등록된 keyword 가져오기"""
            channel_id = interaction.channel.id if isinstance(interaction.channel, discord.TextChannel) else interaction.channel.parent_id
            result = self.channel_manager.get_keyword(channel_id, interaction.user.id)
            await interaction.response.send_message(result)

#         @self.bot.command(name="help", description="사용 가능한 명령어 목록을 보여줍니다.")
#         async def help(ctx):
#             """help message 전송"""
#             help_message=f"""
# - 채널 등록: /register
# - 채널 해제: /unregister
# - 키워드 등록: /add_keyword 키워드
# - 키워드 삭제: /del_keyword 키워드
# - 등록한 키워드 보기: /get_keyword
# - 알람 받을 스레드 생성 : /make_keyword_thread 스레드명 (default = Remind)
# - 인기글 스레드 생성 /make_trend_thread 스레드명 (default = 인급딜)
# - 스레드 삭제 /del_thread (해당 스레드에서 입력하세요!)
# 키워드 등록 시 키워드가 포함된 글은 {ctx.author.mention} 멘션이 갑니다!
# """
#             await ctx.send(help_message)
        @self.bot.tree.command(name="help", description="사용 가능한 명령어 목록을 보여줍니다.")
        async def help(interaction: discord.Interaction):
            """help message 전송"""
            help_message = f"""
- 채널 등록: /register
- 채널 해제: /unregister
- 키워드 등록: /add_keyword 키워드
- 키워드 삭제: /del_keyword 키워드
- 등록한 키워드 보기: /get_keyword
- 알람 받을 스레드 생성: /make_keyword_thread 스레드명
- 인기글 스레드 생성: /make_trend_thread 스레드명
- 스레드 삭제: /del_thread (해당 스레드에서 입력하세요!)
키워드 등록 시 키워드가 포함된 글은 {interaction.user.mention} 멘션이 갑니다!
        """
            await interaction.response.send_message(help_message)

        # @self.bot.command(name="make_keyword_thread")
        # async def make_thread(ctx, thread_name = "Remind"):
        #     """멘션달린 메세지만 모아두는 thread 생성"""
        #     # 스레드 생성
        #     thread = await ctx.channel.create_thread(
        #         name=thread_name,  # 스레드 이름
        #         auto_archive_duration=10080,  # 자동 아카이브 기간 (분 단위, 60분 = 1시간)
        #         type=discord.ChannelType.public_thread  # 공개 스레드
        #     )
        #     thread_id = thread.id
        #     result = self.channel_manager.make_thread(ctx.channel.id, ctx.author.id, thread_id, thread_name, "threads")
        #     await ctx.send(result)
            
        #     # 스레드에 메시지 보내기
        #     await thread.send(f"스레드 '{thread_name}'가 생성되었습니다!")
            
        #     await thread.add_user(ctx.author)
            
        # @self.bot.command(name="make_trend_thread")
        # async def make_thread(ctx, thread_name = "인급딜"):
        #     """댓글 수가 많은 메세지만 모아두는 thread 생성"""
        #     # 스레드 생성
        #     thread = await ctx.channel.create_thread(
        #         name=thread_name,  # 스레드 이름
        #         auto_archive_duration=10080,  # 자동 아카이브 기간 (분 단위, 60분 = 1시간)
        #         type=discord.ChannelType.public_thread  # 공개 스레드
        #     )
        #     thread_id = thread.id
        #     result = self.channel_manager.make_thread(ctx.channel.id, ctx.author.id, thread_id, thread_name, "trend_threads")
        #     await ctx.send(result)
            
        #     # 스레드에 메시지 보내기
        #     await thread.send(f"스레드 '{thread_name}'가 생성되었습니다!")
            
        #     await thread.add_user(ctx.author)

        # @self.bot.command(name="del_thread")
        # async def del_thread(ctx):
        #     """thread 삭제"""
        #     user_id = ctx.author.id
        #     if isinstance(ctx.channel, discord.Thread):
        #         thread_id = ctx.channel_id
        #         await ctx.channel.delete(reason="커맨드로 삭제")
        #         result = self.channel_manager.del_thread(cthread_id)
        #         await ctx.send("쓰레드 삭제 완료!", ephemeral=True)
        #     else:
        #         await ctx.send("이 명령어는 쓰레드 안에서만 실행할 수 있어요.", ephemeral=True)
        
        @self.bot.tree.command(name="make_keyword_thread", description="키워드 알람 스레드를 생성합니다.")
        async def make_keyword_thread(interaction: discord.Interaction, thread_name: str = "Remind"):
            """멘션달린 메세지만 모아두는 thread 생성"""
            thread = await interaction.channel.create_thread(
                name=thread_name,
                auto_archive_duration=10080,
                type=discord.ChannelType.public_thread
            )
            thread_id = thread.id
            result = self.channel_manager.make_thread(interaction.channel.id, interaction.user.id, thread_id, thread_name, "threads")
            await interaction.response.send_message(result)
            
            await thread.send(f"스레드 '{thread_name}'가 생성되었습니다!")
            await thread.add_user(interaction.user)

        @self.bot.tree.command(name="make_trend_thread", description="인기글 스레드를 생성합니다.")
        async def make_trend_thread(interaction: discord.Interaction, thread_name: str = "인급딜"):
            """댓글 수가 많은 메세지만 모아두는 thread 생성"""
            thread = await interaction.channel.create_thread(
                name=thread_name,
                auto_archive_duration=10080,
                type=discord.ChannelType.public_thread
            )
            thread_id = thread.id
            result = self.channel_manager.make_thread(interaction.channel.id, interaction.user.id, thread_id, thread_name, "trend_threads")
            await interaction.response.send_message(result)
            
            await thread.send(f"스레드 '{thread_name}'가 생성되었습니다!")
            await thread.add_user(interaction.user)

        @self.bot.tree.command(name="del_thread", description="현재 스레드를 삭제합니다.")
        async def del_thread(interaction: discord.Interaction):
            """thread 삭제"""
            if isinstance(interaction.channel, discord.Thread):
                thread_id = interaction.channel.id  # ctx.channel_id -> interaction.channel.id로 수정
                result = self.channel_manager.del_thread(thread_id)  # cthread_id 오타 수정
                await interaction.response.send_message("스레드를 삭제합니다...", ephemeral=True)
                await interaction.channel.delete(reason="커맨드로 삭제")
            else:
                await interaction.response.send_message("이 명령어는 스레드 안에서만 실행할 수 있어요.", ephemeral=True)

    def insert_to_item_links_table(self, message, site_name):
        table_name = site_name.lower() + "_item_links"
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # 중복 체크
            cursor.execute(
                f"SELECT 1 FROM {table_name} WHERE item_link = %s",
                (message["item_link"],)
            )
            if cursor.fetchone():
                logging.info(f"Duplicate item_link found: {message['item_link']}")
                return False
                
            cursor.execute(
                f"INSERT INTO {table_name} (item_link) VALUES (%s);",
                (message["item_link"],)
            )
            conn.commit()
            return True
        
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Error inserting to item_links: {e}")
            return False
            
        finally:
            if conn:
                conn.close()
            
    def insert_to_trend_item_links_table(self, message, site_name):
        table_name = site_name.lower() + "_trend_item_links"
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # 중복 체크
            cursor.execute(
                f"SELECT 1 FROM {table_name} WHERE trend_item_link = %s",
                (message,)
            )
            if cursor.fetchone():
                logging.info(f"Duplicate trend_item_link found: {message}")
                return False
                
            cursor.execute(
                f"INSERT INTO {table_name} (trend_item_link) VALUES (%s);",
                (message,)
            )
            conn.commit()
            return True
        
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Error inserting to trend_item_links: {e}")
            return False
            
        finally:
            if conn:
                conn.close()
                
    async def redis_subscribe(self, channel, handler):
        while True:
            redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True,
            )
            pubsub = redis_client.pubsub()
            try:
                await pubsub.subscribe(channel)
                logging.info(f"Subscribed to Redis channel: {channel}")
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        await handler(message["data"])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.error(f"Redis consumer error on {channel}: {e}")
                await asyncio.sleep(1)
            finally:
                with suppress(Exception):
                    await pubsub.unsubscribe(channel)
                with suppress(Exception):
                    await pubsub.aclose()
                with suppress(Exception):
                    await redis_client.aclose()
                        
    def is_error_message(self, body):
        # 에러 메시지 체크
        if body.get("created_at") == "err" or body.get("item_name") == "err":
            logging.info(f"Error Incomplete content : {body}")
            return True
        else:
            return False
        
    def is_duplicated_message(self, site_name, body):
        # 중복 체크
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute(f'''
                    SELECT id FROM {site_name.lower()}
                    WHERE item_link = %s
                ''', (body["item_link"],))
                result = cur.fetchone()
        except Exception as e:
            logging.error(f"Error checking duplicate message: {e}")
            result = False
        finally:
            if conn:
                conn.close()
                
        if result:
            logging.info(f"중복된 메시지 스킵: {body['item_name']}")
            return True
        else:
            return False

    def is_duplicated_trend_message(self, site_name, body):
        # 중복 체크
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute(f'''
                    SELECT id FROM {site_name.lower()}_trend_item_links
                    WHERE trend_item_link = %s
                ''', (body,))
                result = cur.fetchone()
        except Exception as e:
            logging.error(f"Error checking duplicate message: {e}")
            result = False
        finally:
            if conn:
                conn.close()
        if result:
            logging.info(f"중복된 메시지 스킵: {body}")
            return True
        else:
            return False
    
    def get_message_id_from_message_log(self, item_link, channel_id):
        """message_logs 테이블에서 message fetch"""
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT message_id FROM message_logs
                    WHERE item_link = %s AND channel_id = %s
                ''', (item_link, str(channel_id)))
                row = cur.fetchone()
                result = row[0] if row else None
        except Exception as e:
            logging.error(f"Error get message from meesage log: {e}")
            result = None
        finally:
            if conn:
                conn.close()
        return result
        
    async def process_trend_message(self, raw_msg):
        try:
            data = json.loads(raw_msg) if isinstance(raw_msg, str) else raw_msg
            site_name = data.get("site")
            links = data.get("links", [])
            if not site_name or not links:
                logging.error(f"Invalid trend payload: {data}")
                return

            for item_link in links:
                is_duplicated = await asyncio.to_thread(
                    self.is_duplicated_trend_message,
                    site_name,
                    item_link,
                )
                if is_duplicated:
                    logging.info(f"duplicated trend message: {site_name}, {item_link}")
                    continue

                await asyncio.to_thread(
                    self.insert_to_trend_item_links_table,
                    item_link,
                    site_name,
                )
                trend_thread_list = await asyncio.to_thread(
                    self.channel_manager.get_trend_thread_channel_user
                )

                for thread_id, channel_id, user_id in trend_thread_list:
                    message_id = await asyncio.to_thread(
                        self.get_message_id_from_message_log,
                        item_link,
                        channel_id,
                    )
                    if not message_id:
                        continue
                    channel = self.bot.get_channel(channel_id)
                    if not channel:
                        continue
                    message = await channel.fetch_message(message_id)
                    if not message.embeds:
                        continue
                    embed = message.embeds[0]
                    embed.color = discord.Color.red()
                    thread = self.bot.get_channel(thread_id)
                    if thread:
                        await thread.send(embed=embed)
        except Exception as e:
            logging.error(f"Error processing trend message: {e}")
            print("".join(traceback.format_exception(type(e), e, e.__traceback__)))

    async def poll_redis_trend(self):
        logging.info("Starting Redis trend polling...")
        tasks = [
            asyncio.create_task(
                self.redis_subscribe(channel, self.process_trend_message),
                name=f"trend:{channel}",
            )
            for channel in self.trend_channels
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    def _parse_crawl_payload(self, raw_msg):
        data = json.loads(raw_msg) if isinstance(raw_msg, str) else raw_msg
        site_name = data.get("site")
        body = data.get("data", data)

        if not site_name or not isinstance(body, dict):
            raise ValueError(f"invalid message payload: {data}")

        return site_name, body

    def _should_skip_message(self, site_name, body):
        if self.is_error_message(body):
            logging.info(f"skip error message: {body}")
            return True

        if self.is_duplicated_message(site_name, body):
            logging.info(f"duplicated message: {site_name}, {body.get('item_link')}")
            return True

        return False

    async def _enrich_message(self, site_name, body):
        body["pred_category"] = await self.classify_tag(body)
        body["created_at"] = self.get_adjusted_timestamp(body["created_at"], site_name)
        return body

    async def _publish_to_channels(self, embed, item_link):
        channels = self.channel_manager.get_active_channels()
        embeds_json = json.dumps([embed.to_dict()], ensure_ascii=False)

        for channel_id in channels:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                continue
            try:
                channel_msg = await channel.send(embed=embed)
                self.channel_manager.log_message(
                    channel_id=channel_id,
                    message_id=channel_msg.id,
                    embeds=embeds_json,
                    item_link=item_link,
                )
            except Exception as e:
                logging.error(f"Error sending message to channel {channel_id}: {e}")

    async def _publish_to_threads(self, embed, body, item_link):
        thread_info_dict = self.channel_manager.map_thread_to_user_and_keyword(body)
        embeds_json = json.dumps([embed.to_dict()], ensure_ascii=False)

        for thread_id in thread_info_dict:
            thread = self.bot.get_channel(thread_id)
            if not (thread and isinstance(thread, discord.Thread)):
                continue

            guild = thread.guild
            users_to_mention = defaultdict(list)
            for user_id, kw in thread_info_dict[thread_id]:
                users_to_mention[user_id].append(kw)
            if not users_to_mention:
                continue

            try:
                thread_msg = await thread.send(embed=embed)
            except Exception as e:
                logging.error(f"Error sending embed to thread {thread_id}: {e}")
                continue

            for user_id, kws in users_to_mention.items():
                user = guild.get_member(user_id)
                if not user:
                    continue
                try:
                    keywords_str = ", ".join(kws)
                    await thread.send(f"{user.mention} {embed.title}\nkeywords: {keywords_str}")
                except Exception as e:
                    logging.error(f"Error sending keyword mention to thread {thread_id} for user {user_id}: {e}")

            self.channel_manager.log_message(
                channel_id=thread_id,
                message_id=thread_msg.id,
                embeds=embeds_json,
                item_link=item_link,
            )

    def _persist_message(self, body, site_name):
        self.insert_to_item_links_table(body, site_name)
        message_id = self.channel_manager.save_message(body, site_name)
        if message_id is None:
            raise RuntimeError("메시지 저장 실패")

    async def process_crawl_message(self, raw_msg):
        try:
            site_name, body = self._parse_crawl_payload(raw_msg)

            if self._should_skip_message(site_name, body):
                return

            body = await self._enrich_message(site_name, body)
            item_link = body["item_link"]
            embed = self.transform_message(body, site_name)

            await self._publish_to_channels(embed, item_link)
            await self._publish_to_threads(embed, body, item_link)
            self._persist_message(body, site_name)

            logging.info(f"Successfully processed message: {body['item_name']}")
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode message body: {e}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    async def poll_redis_crawl(self):
        logging.info("Starting Redis crawl polling...")
        tasks = [
            asyncio.create_task(
                self.redis_subscribe(channel, self.process_crawl_message),
                name=f"crawl:{channel}",
            )
            for channel in self.crawl_channels
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def start_background_tasks(self):
        if any(not task.done() for task in self._background_tasks):
            return
        self._background_tasks = [
            asyncio.create_task(self.poll_redis_crawl(), name="poll_redis_crawl"),
            asyncio.create_task(self.poll_redis_trend(), name="poll_redis_trend"),
        ]

    async def stop_background_tasks(self):
        if not self._background_tasks:
            return
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def classify_tag(self, message):
        """아이템 분류 태그 생성"""
        try:
            system_message = """
너는 이커머스 상품 카테고리 분류 전문가다.
아래 기준에 따라 상품 정보를 분석하고 카테고리 태그만 출력하라.

[분류 기준]
- 대분류: 상품의 가장 상위 개념 (예: 식품, 생활용품, 가전, 패션, 뷰티, 반려동물 등)
- 중분류: 대분류 하위의 용도 또는 상품군
- 소분류: 실제 상품을 가장 구체적으로 설명하는 카테고리

[태그 생성 규칙]
- 최대 3개 태그만 생성
- 각 태그는 반드시 한글이며 '#'으로 시작
- 태그 순서는 반드시 대분류 → 중분류 → 소분류
- 기존 상품태그와 문자열이 완전히 동일한 태그는 생성하지 말 것
- 의미가 유사하더라도 문자열이 다르면 생성 가능
- 불확실한 경우 가장 일반적인 분류를 선택

[출력 규칙 - 매우 중요]
- 반드시 태그만 출력할 것
- '#대분류', '#중분류', '#소분류' 와 같은 플레이스홀더 출력 금지
- 판단 과정, 고민, 수정 문장, 영어, 특수문자 출력 금지
- 출력은 한 줄이며 공백으로만 구분
- 쉼표, 줄바꿈, 설명 문장 절대 금지

[올바른 출력 예]
#가전 #PC부품 #그래픽카드
#음식 #육류 #삼겹살
"""

            # GPT 프롬프트 구성
            prompt = f"""
[상품 정보]
상품명: {message['item_name']}
상품설명: {message['content']}
기존상품태그: {message['category']}

응답:
"""

            # ChatGPT API 호출
            response = await self.openai_client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_completion_tokens=500
            )
            logging.info(str(response))
            
            # 응답에서 태그 추출
            tags = response.choices[0].message.content.strip()
            logging.info(f"Generated tags for {message['item_name']}: {tags}")
            return tags

        except Exception as e:
            logging.error(f"Error generating tags: {e}")
            return "#기타"  # 에러 발생 시 기본 태그 반환

    def preprocess_raw_content(self, raw_content):
        """본문을 정리하고 길이를 제한합니다."""
        content = raw_content.strip()
        content = re.sub(r"\n+", "\n", content)
        if len(content) > 1024:  # 1024자로 제한
            content = content[:1021] + "..."
        return content

    def get_adjusted_timestamp(self, created_at_str, site_name):
        """문자열 created_at을 파싱하고, (원문 TZ -> KST)로 변환한 datetime을 반환"""
        try:
            if not created_at_str:
                return pendulum.now("Asia/Seoul")

            s = created_at_str.strip()

            if site_name == "RULI_WEB":
                # 예: "2024.01.02 (12:34:56)" -> "2024.01.02 12:34"
                s = re.sub(
                    r'(\d{4}\.\d{2}\.\d{2}) \((\d{2}:\d{2}):\d{2}\)',
                    r'\1 \2',
                    s
                )

            # 공통 정규화
            s = s.replace(".", "-")

            # 사이트별 "원본 타임존" 지정
            source_tz = "Asia/Seoul"
            if site_name == "ARCA_LIVE":
                # 기존 코드가 +9시간을 했던 걸 보면 원본이 UTC였다고 가정
                source_tz = "UTC"

            dt = pendulum.parse(s, strict=False, tz=source_tz)
            return dt.in_timezone("Asia/Seoul")

        except Exception as e:
            logging.error(f"Error parsing created_at ({site_name}): {created_at_str} / {e}")
            return pendulum.now("Asia/Seoul")

    
    def transform_message(self, message, site_name):
        """메시지를 Discord 임베드 형식으로 변환"""
        try:
            # raw content 전처리
            content = self.preprocess_raw_content(message["content"])
            
            # created_at 조정
            # created_at = self.get_adjusted_timestamp(message["created_at"], site_name)

            # 임베드 생성
            embed = discord.Embed(
                title=message['item_name'][:256],  # Discord 제목 길이 제한
                description=f"{message['pred_category']}",  # 이미 생성된 태그 사용
                color=discord.Color.blue(),
                timestamp=message['created_at']
            )

            # 필드 추가
            embed.add_field(
                name="원문 링크",
                value=message["item_link"],
                inline=True
            )
            embed.add_field(
                name="구매 링크",
                value=message["shopping_mall_link"],
                inline=True
            )
            embed.add_field(
                name="본문",
                value=content,
                inline=False
            )

            # 푸터 추가
            embed.set_footer(text=f"{site_name}")

            return embed

        except Exception as e:
            logging.error(f"Error transforming message: {e}")
            # 에러 발생 시 기본 임베드 반환
            error_embed = discord.Embed(
                title="Error Processing Message",
                description="메시지 처리 중 오류가 발생했습니다.",
                color=discord.Color.red()
            )
            return error_embed

    def run(self):
        """봇 실행"""
        try:
            # 초기 설정
            self.setup_logging()
            self.setup_environment()
            self.setup_bot()
            
            # 봇 실행
            logging.info("Starting bot...")
            self.bot.run(os.environ.get("DISCORD_TOKEN"))
            
        except Exception as e:
            logging.error(f"Fatal error: {e}")
            raise
        finally:
            logging.info("Bot shutdown completed")

if __name__ == "__main__":
    bot = HotDealBot()
    bot.run()
