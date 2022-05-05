# various import
import pymongo
import pika  # rabbitmq
import time
import pickle
import zlib
import orjson


# MongoDB client
mongo_client = pymongo.MongoClient(connect=False)
mydb = mongo_client["paper"]
mycol = mydb["colname"]


def set_rabbit():
    # rabbitMQ client(reason of localhost - this is subscriber)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # set receiving queue
    channel.queue_declare(queue='parquet')
    return channel


def callback(ch, method, properties, body):

    data = orjson.loads(body)[0]
    if 'fin' in data.keys():
        print("rabbit endtime: ", time.time() - start)
        quit()
    mycol.insert_one(data)
    #data = pickle.loads(zlib.decompress(body))
    # if str(type(data)) == "<class 'list'>":
    #     print("rabbit endtime: ", time.time() - start)
    #     quit()
    #mongo_insert(data)


# mongo raw 데이터가 입력되는 Function
def mongo_insert(data):
    dict_data = data.to_dict(orient='records')
    document = {
        "data": dict_data
    }
    mycol.insert_one(document)






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
