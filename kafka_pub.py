from kafka import KafkaProducer
from json import dumps
import time

producer = KafkaProducer(
    acks=0, # 메시지 전송 완료에 대한 체크
    compression_type='gzip', # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
    bootstrap_servers=['localhost:9092'], # 전달하고자 하는 카프카 브로커의 주소 리스트
    value_serializer=lambda x:dumps(x).encode('utf-8') # 메시지의 값 직렬화
)

start = time.time()

for i in range(1000):
    data = {'str' : 'result'+str(i)}
    producer.send('test', value=data)
    producer.flush() # 

print('[Done]:', time.time() - start)