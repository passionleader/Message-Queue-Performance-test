'''
논문 실험용 코드
'''
import os
import time  # processing performance
import pyarrow.parquet as pq  # read parquet
from orjson import dumps
from tqdm import tqdm
import stomp

# 실험 변수
parquet_file_path = "../dataset.parquet"
rowBatchSize = 1280

# active MQ 서버 정보 선언
user = os.getenv("ACTIVEMQ_USER") or "admin"
password = os.getenv("ACTIVEMQ_PASSWORD") or "password"
host = os.getenv("ACTIVEMQ_HOST") or "114.70.212.154"
port = os.getenv("ACTIVEMQ_PORT") or 61613
destination = "/topic/parquet"  # 토픽 세팅


# Loop through generator obj 제너레이터를 통한 반복 전송
def slice_generator():
    list = range(0, len(dataset), rowBatchSize)  # 범위만 담고 있음
    for y in tqdm(list):  # 범위 만큼 반복
        yield y  # 반환


# make connection to activeMQ Server
conn = stomp.Connection(host_and_ports=[(host, port)])
conn.connect(login=user, passcode=password)

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
    conn.send(destination, transferSlice, persistent='false')

# 닫기, 시간 측정 마무리
conn.send(destination, "SHUTDOWN", persistent='true')
print(" endtime:", time.time() - start)
conn.disconnect()
