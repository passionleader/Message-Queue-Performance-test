import pika

# make connection to a broker on the local machine
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='parquet')


def callback(ch, method, properties, body):
    if body is None:
        quit()

channel.basic_consume(queue='parquet',
                      auto_ack=True,
                      on_message_callback=callback)


print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
print("flush completed")