# various import
import pymongo
import pika  # rabbitmq
import time
import asyncio
from nats.aio.client import Client as NATS
import pickle
import zlib
import orjson


# # MongoDB client
# mongo_client = pymongo.MongoClient(connect=False)
# mydb = mongo_client["paper"]
# mycol = mydb["colname"]
#
#
# def set_rabbit():
#     # rabbitMQ client(reason of localhost - this is subscriber)
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     # set receiving queue
#     channel.queue_declare(queue='parquet')
#     return channel
#
#
# def callback(ch, method, properties, body):
#     data = pickle.loads(zlib.decompress(body))
#     if str(type(data)) == "<class 'list'>":
#         print("rabbit endtime: ", time.time() - start)
#         quit()
#     mongo_insert(data)
#
#
# # mongo raw 데이터가 입력되는 Function
# def mongo_insert(data):
#     sec = data.iloc[0]["100ms"]//10  # can delete this and set it to 'id'!
#     dict_data = data.to_dict(orient='records')
#     document = {
#         "_id": 'DAQ+' + str(sec),
#         "group": "DAQ",
#         "sec": int(sec),
#         "data": dict_data
#     }
#     mycol.insert_one(document)
#
#
# # rabbitMQ server setting
# channel = set_rabbit()
#
# # RabbitMQ에 parquet 큐에서 메시지를 수신한다고 알리며 "callback"을 수행
# channel.basic_consume(queue='parquet',
#                       auto_ack=True,
#                       on_message_callback=callback)
# # enter to start
# input("Enter to start")
# # info
# print('start consuming')
# # Time checker
# start = time.time()
# # start
# channel.start_consuming()
#
#
# ---



# ref : https://github.com/nats-io/asyncio-nats-examples
# [begin subscribe_json]
import asyncio
import json
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout
import pickle
import zlib

# MongoDB client
mongo_client = pymongo.MongoClient(connect=False)
mydb = mongo_client["paper"]
mycol = mydb["colname"]

# mongo raw 데이터가 입력되는 Function
def mongo_insert(data):
    dict_data = data.to_dict(orient='records')
    document = {
        "data": dict_data
    }
    mycol.insert_one(document)


async def run(loop):
    nc = NATS()
    # NATS server setting
    await nc.connect(servers=["nats://localhost:4222"], loop=loop)
    #print(nc.max_payload)
    async def message_handler(msg):
        data = pickle.loads(zlib.decompress(msg.data))
        if str(type(data)) == "<class 'list'>":
            print("kafka endtime: ", time.time() - start)
            quit()
        mongo_insert(data)

        # data = orjson.loads(msg.data)[0]
        # if 'fin' in data.keys():
        #     print("rabbit endtime: ", time.time() - start)
        #     quit()
        # mycol.insert_one(data)


        # data = json.loads()  # original

    sid = await nc.subscribe("milling", cb=message_handler)



    await asyncio.sleep(10, loop=loop)  # loop back fnc. ignore sleep second if you use while at the main
    await nc.close()  # prevents message duplicate(flush)


# [end subscribe_json]

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    input("press enter to start")
    start = time.time()
    while True:
        loop.run_until_complete(run(loop))
    loop.close()

# scuucedd