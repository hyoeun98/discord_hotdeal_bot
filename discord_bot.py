# This example requires the 'message_content' intent.

import discord
from kafka import KafkaConsumer
from json import loads

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

consumer = KafkaConsumer(
    'test', # 토픽명
    bootstrap_servers=['localhost:9092'], # 카프카 브로커 주소 리스트
    auto_offset_reset='earliest', # 오프셋 위치(earliest:가장 처음, latest: 가장 최근)
    enable_auto_commit=True, # 오프셋 자동 커밋 여부
    group_id='test-group', # 컨슈머 그룹 식별자
    value_deserializer=lambda x: loads(x.decode('utf-8')), # 메시지의 값 역직렬화
    consumer_timeout_ms=10000 # 데이터를 기다리는 최대 시간
)
@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')
    # kafka subcribe
    # await message.channel.send('Hello!')
@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('$hello'):
        for i in consumer:
            await message.channel.send(i)

client.run('my_token')
