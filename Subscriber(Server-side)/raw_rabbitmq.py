'''
subscriber
4개 정도로 consumer를 여러 개 구성합니다
timestamp column is based
'''
# various import
import pika
import pymongo
import orjson
import time

# MongoDB client
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["paper"]
mycol = mydb["colname"]


def set_rabbit():
    # rabbitMQ client(reason of localhost - this is subscriber)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # set receiving queue
    channel.queue_declare(queue='parquet')
    return channel


def callback(ch, method, properties, body):
    received_data = orjson.loads(body)
    if "fin" in received_data:
        print("rabbit endtime: ", time.time() - start)
        quit()
    mycol.insert_many(received_data)


# rabbitMQ server setting
channel = set_rabbit()

# RabbitMQ에 parquet 큐에서 메시지를 수신한다고 알리며 "callback"을 수행
channel.basic_consume(queue='parquet',
                      auto_ack=True,
                      on_message_callback=callback)
# enter to start
input("Enter to start")
# info
print('start consuming')
# Time checker
start = time.time()
# start
channel.start_consuming()
