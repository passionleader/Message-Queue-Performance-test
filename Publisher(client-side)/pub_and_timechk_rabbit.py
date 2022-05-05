'''
논문 실험용 코드
'''
import time  # processing performance
import pyarrow.parquet as pq  # read parquet
import pika  # RabbitMQ module
from orjson import dumps
from tqdm import tqdm

# 실험 변수
parquet_file_path = "../dataset.parquet"
rowBatchSize = 12800

# rabbitMQ 서버 정보 선언
server_host = '114.70.212.154'
rabbit_account = 'admin'
rabbit_password = '12345'


# rabbitMQ Setting
def connect_rabbit():
    # set account - 서버측에서 만든 account 사용해서 접속 가능하게 함
    credentials = pika.PlainCredentials(rabbit_account, rabbit_password)
    # make connection to a broker on the Linux
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=server_host, credentials=credentials))
    channel = connection.channel()
    # before sending, make sure the recipient queue exists.(unless, rabbit will delete message)
    channel.queue_declare(queue='parquet')
    print("[RabbitMQ]successfully connected to RabbitMQ server")
    return channel, connection


# Loop through generator obj 제너레이터를 통한 반복 전송
def slice_generator():
    list = range(0, len(dataset), rowBatchSize)  # 범위만 담고 있음
    for y in tqdm(list):  # 범위 만큼 반복
        yield y  # 반환


# make connection to rabbitMQ Server
channel, connection = connect_rabbit()

# DataFrame 형태로 변환
table = pq.read_table(parquet_file_path)
dataset = table.to_pandas()

# 반복용 generator object for loop(0, 12800, 25600, ...), 주의:일회용
generatorObj = slice_generator()

# 시작 입력
input("반드시 subscriber 먼저 실행시켜주세요 > 확인:엔터")

# 시간 측정
start = time.time()  # 시간 측정

# publish dataset - 제너레이터 객체를 통한 반복으로 파일 하나 치의 데이터를 1초 분량씩(cnc-30 / daq-12800 개씩) 전송
for i in generatorObj:
    transferSlice = dataset[i:i + rowBatchSize].to_json(orient='records', double_precision=15)
    channel.basic_publish(exchange='', routing_key='parquet', body=transferSlice)

# 마지막 알리기
fin_alert = {"fin": 1}
fin = dumps(fin_alert)
channel.basic_publish(exchange='', routing_key='parquet', body=fin)

# 닫기, 시간 측정 마무리
print(" endtime:", time.time() - start)
connection.close()
