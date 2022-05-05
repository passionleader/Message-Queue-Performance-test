'''
논문 실험용 코드
'''
import time  # processing performance
import pyarrow.parquet as pq  # read parquet
import pymongo
from tqdm import tqdm  # loop display
import orjson

# 실험 변수
parquet_file_path = "../dataset.parquet"
rowBatchSize = 12800

# 몽고 서버
client = pymongo.MongoClient("mongodb://114.70.212.154:27017")
mydb = client["paper"]
mycol = mydb["colname"]


# Loop through generator obj 제너레이터를 통한 반복 전송
def slice_generator():
    list = range(0, len(dataset), rowBatchSize)  # 범위만 담고 있음
    for y in tqdm(list):  # 범위 만큼 반복
        yield y  # 반환


# DataFrame 형태로 변환
table = pq.read_table(parquet_file_path)
dataset = table.to_pandas()

# 반복용 generator object for loop(0, 12800, 25600, ...), 주의:일회용
generatorObj = slice_generator()

# 시간 측정
start = time.time()  # 시간 측정

# publish dataset - 제너레이터 객체를 통한 반복으로 파일 하나 치의 데이터를 1초 분량씩(cnc-30 / daq-12800 개씩) 전송
for i in generatorObj:
    #transferSlice = dataset[i:i + rowBatchSize].to_dict(orient='records')
    transferSlice = dataset[i:i + rowBatchSize].to_json(orient='records', double_precision=15)
    redo = orjson.loads(transferSlice)
    mycol.insert_many(redo)

# 닫기, 시간 측정 마무리
print(" endtime:", time.time() - start)
