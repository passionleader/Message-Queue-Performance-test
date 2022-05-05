import pandas as pd
from multiprocessing import Process, Queue
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from tqdm import tqdm  # loop display
import pyarrow.parquet as pq  # read parquet
from time import time
import orjson


# 실험 변수
print("File Loading Please wait")
parquet_file_path = "../dataset.parquet"
rowBatchSize = 120
channel_name = "Sensor"


# 설정 가능 서버 변수
bucket = "PAPER"
exp_name = "exp1"

# 고정 서버 클라이언트 정보
token = "uWEUQi6A4jvuKrgktEZHaBENiUO2uAFXbwmG7kg8ZwFMKgFTTpD8PXnWbY6BDBLUYt0jTM5bxxiJDco_0jyyKw=="
org = "kitech"
client = InfluxDBClient(url="http://114.70.212.154:8086", token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)


# Loop through generator obj 제너레이터
def slice_generator():
    list = range(0, len(dataset), rowBatchSize)  # 범위만 담고 있음
    for y in tqdm(list):  # 범위 만큼 반복
        yield y  # 반환


# DataFrame 형태로 변환
table = pq.read_table(parquet_file_path)
dataset = table.to_pandas()

# 반복용 generator object for loop(0, 12800, 25600, ...), 주의:일회용
generatorObj = slice_generator()


# 태그 키 등 설정하기
dataset["seconds"] = dataset["Timestamp"].apply(int)
dataset["channel"] = "Sensor"
# 인덱스 설정하기(타임스탬프가 반드시 인덱스로 와야 DB Insert 가능)
dataset["Timestamp"] = (dataset["Timestamp"]*(10**9)).apply(int)
dataset.set_index("Timestamp", inplace=True)

# 시간 측정
start = time()  # 시간 측정

# publish dataset - 제너레이터 객체를 통한 12800 개씩) 전송
for i in generatorObj:
    transferSlice = dataset[i:i + rowBatchSize]
    # print(transferSlice)
    write_api.write(
        bucket=bucket, org=org,
        data_frame_measurement_name=exp_name,
        #data_frame_tag_columns=['seconds'],
        data_frame_tag_columns=['seconds', 'channel'],
        #tags={"ch_name": channel_name},
        record=transferSlice
    )

# 닫기, 시간 측정 마무리
print(" endtime:", time() - start)
