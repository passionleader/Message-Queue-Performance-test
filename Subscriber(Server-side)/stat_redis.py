import redis
import zlib
import pickle
from kafka import KafkaConsumer  # 컨슈머
import orjson
import pickle
import zlib
import pymongo
import time

# This code referred below site partially.
# https://chan7ee.tistory.com/entry/python-%EC%97%90%EC%84%9C-redis%EB%A5%BC-%EB%A9%94%EC%8B%9C%EC%A7%80-queue%EB%A1%9C-%EC%82%AC%EC%9A%A9

class RedisQueue(object):
    """
        Redis Lists are an ordered list, First In First Out Queue
        Redis List pushing new elements on the head (on the left) of the list.
        The max length of a list is 4,294,967,295
    """
    def __init__(self, name, **redis_kwargs):
        """
            host='localhost', port=6379, db=0
        """
        self.key = name
        self.rq = redis.Redis(**redis_kwargs)

    def size(self): # 큐 크기 확인
        return self.rq.llen(self.key)

    def isEmpty(self): # 비어있는 큐인지 확인
        return self.size() == 0

    def put(self, element): # 데이터 넣기
        self.rq.lpush(self.key, element) # left push

    def get(self, isBlocking=False, timeout=None): # 데이터 꺼내기
        if isBlocking:
            element = self.rq.brpop(self.key, timeout=timeout) # blocking right pop
            element = element[1] # key[0], value[1]
        else:
            element = self.rq.rpop(self.key) # right pop
        return element

    def get_without_pop(self): # 꺼낼 데이터 조회
        if self.isEmpty():
            return None
        element = self.rq.lindex(self.key, -1)
        return element


# mongo raw 데이터가 입력되는 Function
def mongo_insert(data):
    dict_data = data.to_dict(orient='records')
    document = {
        "data": dict_data
    }
    mycol.insert_one(document)


# REDIS CONNECT
q = RedisQueue('myq', host='114.70.212.154', port=6379, db=0,password='wktmalsck')

# MongoDB client
mongo_client = pymongo.MongoClient(connect=False)
mydb = mongo_client["paper"]
mycol = mydb["colname"]

# start!!
input("press enter to start!!")
start = time.time()

# message get
while(True):
    msg = q.get(isBlocking=True) # 큐가 비어있을 때 대기
    if msg is not None:

        # data = orjson.loads(msg)[0]
        # if 'fin' in data.keys():
        #     print("radis endtime: ", time.time() - start)
        #     quit()
        # mycol.insert_one(data)

        data = pickle.loads(zlib.decompress(msg))
        if str(type(data)) == "<class 'list'>":
            print("redis endtime: ", time.time() - start)
            quit()
        mongo_insert(data)