# various import
import stomp
import os
import pymongo
import orjson
import time
import sys
import zlib

# MongoDB client
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["paper"]
mycol = mydb["colname"]

# activeMQ에 접근할 계정 설정
user = os.getenv("ACTIVEMQ_USER") or "admin"
password = os.getenv("ACTIVEMQ_PASSWORD") or "password"
host = os.getenv("ACTIVEMQ_HOST") or "localhost"

# 포트 및 수신하는 토픽 설정
port = os.getenv("ACTIVEMQ_PORT") or 61613
destination = "/topic/parquet"  # 수신할 topic 설정


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
            diff = time.time() - self.start
            print("activemq Received %s in %f seconds" % (self.count, diff))
            conn.disconnect()
            sys.exit(0)
        # 메시지 수신 후 변환하여 mongo에 저장한다.
        else:
            # 메시지 수신 파트
            pythonJson = zlib.decompress(orjson.loads(message))
            # 괄호 없애서 dict 만들기
            # insertData = [row for row in pythonJson]
            # 몽고 insert
            mycol.insert_many(pythonJson)


# mqtt 연결
conn = stomp.Connection(host_and_ports=[(host, port)])

# start steady!
input("Enter to start")

# call the class
conn.set_listener('', MyListener(conn))
conn.connect(login=user, passcode=password)

# 함수 호출
conn.subscribe(destination=destination, id=1, ack='auto')

# 첫 메시지가 들어올 때까지 기다립니다.
print("Waiting for messages...")
while 1:
    time.sleep(10)

conn.disconnect()
