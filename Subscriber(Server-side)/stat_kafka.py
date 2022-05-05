# various import
from kafka import KafkaConsumer  # 컨슈머
import orjson
import pickle
import zlib
import pymongo
import time


# mongo raw 데이터가 입력되는 Function
def mongo_insert(data):
    dict_data = data.to_dict(orient='records')
    document = {
        "data": dict_data
    }
    mycol.insert_one(document)

# MongoDB client
mongo_client = pymongo.MongoClient(connect=False)
mydb = mongo_client["paper"]
mycol = mydb["colname"]

# topic, broker list. 속성참고 https://twowinsh87.github.io/etc/2018/08/11/etc-kafka-12/
consumer = KafkaConsumer(
    'parquet',
    fetch_max_bytes=10 * 1024 * 1024,
    max_partition_fetch_bytes=10*1024*1024,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: pickle.loads(zlib.decompress(x)),
    consumer_timeout_ms=30000

)  # 30초동안 입력없으면 종료함

# start!!
input("press enter to start!!")
start = time.time()

# consumer list를 가져온다
print('[begin] get consumer list')



# 첫 한줄을 제외한 나머지 - rowSize씩 데이터 받기
for message in consumer:
    data = orjson.loads(message.value)[0]
    if 'fin' in data.keys():
        print("rabbit endtime: ", time.time() - start)
        quit()
    mycol.insert_one(data)
    #
    # data = pickle.loads(zlib.decompress(message.value))
    # if str(type(data)) == "<class 'list'>":
    #     print("kafka endtime: ", time.time() - start)
    #     quit()
    # mongo_insert(data)




print('[end]30000ms time out')



















