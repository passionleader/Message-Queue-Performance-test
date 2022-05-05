# except DB insert, measure only message broker's performance


# various import
import pika  # rabbitmq
import orjson
import pickle
import zlib
import time
import asyncio
import json
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout
import pickle
import zlib
from kafka import KafkaConsumer  # 컨슈머


# # rabbitmq
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
#
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


######################################################################################################
# # kafka
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
#
#
# # MY OPTION
# # consumer = KafkaConsumer(
# #     'parquet',
# #     fetch_max_bytes=10 * 1024 * 1024,
# #     max_partition_fetch_bytes=10*1024*1024,
# #     bootstrap_servers=['localhost:9092'],
# #     auto_offset_reset='latest',
# #     enable_auto_commit=True,
# #     value_deserializer=lambda x: pickle.loads(zlib.decompress(x)),
# #     consumer_timeout_ms=30000
# #)  # 30초동안 입력없으면 종료함
#
# # DEFAULT OPTION
# consumer = KafkaConsumer(
#     'parquet',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='latest',
#     value_deserializer=lambda x: pickle.loads(zlib.decompress(x)),
#     consumer_timeout_ms=30000
# )  # 30초동안 입력없으면 종료함
#
#
#
# # start!!
# input("press enter to start!!")
#
# start = time.time()
#
# # consumer list를 가져온다
# print('[begin] get consumer list')
#
# # 첫 한줄을 제외한 나머지 - rowSize씩 데이터 받기
# for message in consumer:
#     data = pickle.loads(zlib.decompress(message.value))
#     if str(type(data)) == "<class 'list'>":
#         print("kafka endtime: ", time.time() - start)
#         quit()
#     mongo_insert(data)
#
# print('[end]30000ms time out')
#
###################################################################################
# nats
async def run(loop):
    nc = NATS()
    # NATS server setting
    await nc.connect(servers=["nats://localhost:4222"], loop=loop)
    #print(nc.max_payload)
    async def message_handler(msg):
        # data = pickle.loads(zlib.decompress(msg))  # new
        data = pickle.loads(zlib.decompress(msg.data))
        # data = json.loads()  # original
        mongo_insert(data)

    sid = await nc.subscribe("milling", cb=message_handler)



    await asyncio.sleep(10, loop=loop)  # loop back fnc. ignore sleep second if you use while at the main
    await nc.close()  # prevents message duplicate(flush)


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

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # enter to start
    input("Enter to start")
    # info
    print('start consuming')
    # Time checker
    start = time.time()

    while True:
        loop.run_until_complete(run(loop))
    print("nats endtime: ", time.time() - start)

    loop.close()



