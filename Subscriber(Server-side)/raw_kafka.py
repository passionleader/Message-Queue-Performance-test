'''
subscriber
4개 정도로 consumer를 여러 개 구성합니다
timestamp column is based
'''
# various import
from kafka import KafkaConsumer  # 컨슈머
import pymongo
import orjson
import time

# MongoDB client
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["paper"]
mycol = mydb["colname"]


# topic, broker list. 속성참고 https://twowinsh87.github.io/etc/2018/08/11/etc-kafka-12/
consumer = KafkaConsumer(
    'parquet',
    fetch_max_bytes=10 * 1024 * 1024,
    max_partition_fetch_bytes=10*1024*1024,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: orjson.loads(x.decode('utf-8')),
    consumer_timeout_ms=30000

)  # 30초동안 입력없으면 종료함

# start!!
input("press enter to start!!")
start = time.time()

# consumer list를 가져온다
print('[begin] get consumer list')

# 첫 한줄을 제외한 나머지 - rowSize씩 데이터 받기
for message in consumer:  # json으로 전송 - string으로 수신. 한 묶음이 온다.
    received_data = orjson.loads(message.value)  # mongo에 보내기 위해 str을 딕셔너리 형태로 바꾸기
    if "fin" in received_data:
        print("kafka endtime: ", time.time() - start)
        quit()
    mycol.insert_many(received_data)
    #print(received_data)


print('[end]30000ms time out')