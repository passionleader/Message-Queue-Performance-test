# various import
import pika  # rabbitmq
from kafka import KafkaConsumer  # 컨슈머
import pickle
import zlib
import stomp
import os
import pymongo
import orjson
import time
import sys



# MongoDB client
mongo_client = pymongo.MongoClient(connect=False)
mydb = mongo_client["paper"]
mycol = mydb["colname"]


# activeMQ에 접근할 계정 설정
user = os.getenv("ACTIVEMQ_USER") or "admin"
password = os.getenv("ACTIVEMQ_PASSWORD") or "password"
host = os.getenv("ACTIVEMQ_HOST") or "localhost"


# 포트 및 수신하는 토픽 설정
port = os.getenv("ACTIVEMQ_PORT") or 61613
destination = "/topic/parquet"  # 수신할 topic 설정

# mongo raw 데이터가 입력되는 Function
def mongo_insert(data):
    sec = data.iloc[0]["100ms"]//10  # can delete this and set it to 'id'!
    dict_data = data.to_dict(orient='records')
    document = {
        "_id": 'DAQ+' + str(sec),
        "group": "DAQ",
        "sec": int(sec),
        "data": dict_data
    }
    mycol.insert_one(document)



# listener func
class MyListener(object):
    # 연결 후 수신이 끝날 때까지의 시간을 측정
    def __init__(self, conn):
        self.conn = conn
        self.count = 0
        self.start = time.time()

    # 에러 표시
    def on_error(self, headers, message):
        print('received an error %s' % message)

    # 메시지 수신
    def on_message(self, headers, message):
        # 메시지 전송 완료 후 shutdown 요청에 의해 수신을 끝내는 부분(필요에 의해 제거)
        if message == "SHUTDOWN":
            print("active end time = ", time.time() - stt)
            quit()
        # 메시지 수신 파트
        # data = pickle.loads(zlib.decompress(message))
        # data = orjson.loads(zlib.decompress(message))
        data = orjson.loads(message)[0]

        # 괄호 없애서 dict 만들기
        # insertData = [row for row in pythonJson]
        # 몽고 insert
        mycol.insert_one(data)
        # mongo_insert(data)


# mqtt 연결
conn = stomp.Connection(host_and_ports=[(host, port)])

# start steady!
input("Enter to start")

stt = time.time()
# call the class
conn.set_listener('', MyListener(conn))
conn.connect(login=user, passcode=password)

# 함수 호출
conn.subscribe(destination=destination, id=1, ack='auto')

# 첫 메시지가 들어올 때까지 기다립니다.
print("Waiting for messages...")
while 1:
    time.sleep(30)

conn.disconnect()



