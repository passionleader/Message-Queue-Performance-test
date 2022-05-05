"""
기술통계량 계산 후 1ms 단위로 기술통계 구한 뒤,
한 번에 10개씩 보낸다
"""

# various import
import numpy as np
import scipy.stats
from time import time  # processing performance
import pyarrow.parquet as pq  # read parquet
import pymongo
from tqdm import tqdm  # loop display




# various import
import numpy as np
import scipy.stats
import time  # processing performance
import pyarrow.parquet as pq  # read parquet
from tqdm import tqdm
import asyncio
import pickle
import zlib
from nats.aio.client import Client as NATS
import json


# Loop through generator obj 제너레이터 반복을 통한 기술 통계량 계산
def slice_generator(time_range):
    list = range(time_range)  # 범위만 담고 있음
    for y in tqdm(list):  # 범위 만큼 반복
        yield y  # 반복 할 것 반환


# 기술 통계 계산
def statistic_calc(batch_df):
    statistic = batch_df.groupby('100ms').agg(
        [np.mean, min, max, np.median, np.var, scipy.stats.skew, scipy.stats.kurtosis])
    statistic = statistic.stack(level=0).reset_index()  # 건희는 이게 편해, 채널이 column 으로
    return statistic


# 전송 방법에 따른 함수 구현
def json_pub(df):
    res = df.to_json(orient='records', double_precision=15)
    return res


def dict_pub(df):
    res = df.to_dict(orient='records')
    return res


def byte_pub(df):
    res = zlib.compress(pickle.dumps(df))
    return res


def zlib_pub(df):
    res = zlib.compress(df.to_json(orient='records', double_precision=15))
    return res


def pickle_pub(df):
    res = pickle.dumps(df)
    return res


def mongo_insert(data):
    dict_data = data.to_dict(orient='records')
    document = {
        'data' : dict_data
    }
    mycol.insert_one(document)

# 실험 변수
parquet_file_path = "../dataset.parquet"

# 몽고 서버
client = pymongo.MongoClient("mongodb://114.70.212.154:27017")
mydb = client["paper"]
mycol = mydb["colname"]




# Nats loop by asyncio
loop = asyncio.get_event_loop()


# DataFrame 형태로 변환
table = pq.read_table(parquet_file_path)
dataset = table.to_pandas()

# 기술 통계량 계산 시 그룹화 시킬  millisecond 열 추가
dataset["100ms"] = (dataset["Timestamp"]*10).apply(int)  # 기숱롱계량 구하기 위함
dataset = dataset.drop("Timestamp", axis=1)

# 전송: 본 데이터(100ms 열을 바탕으로 0.1초 분량씩 전송)
# * 100ms 배치의 총 개수 계산 후 그만큼 반복하는 제너레이터 생성
df_start_sec = dataset.loc[0, ['100ms']][0]//10
df_end_sec = dataset.loc[len(dataset) - 1, ['100ms']][0]//10
generatorObj = slice_generator(int(df_end_sec - df_start_sec))

# 200*5 초짜리 기술통계 데이터 만들기
statistic_df_ls = []
print("기술 통계량을 미리 계산합니다")
for i in generatorObj:
    calc = statistic_calc(dataset.loc[dataset['100ms'] // 10 == df_start_sec + i])
    statistic_df_ls.append(calc)
    statistic_df_ls.append(calc)
    statistic_df_ls.append(calc)
    statistic_df_ls.append(calc)
    statistic_df_ls.append(calc)

print(len(statistic_df_ls))

# 시작 입력
input("반드시 subscriber 먼저 실행시켜주세요 > 확인:엔터")

# 시간 측정
start = time.time()  # 시간 측정

for i in tqdm(range(len(statistic_df_ls))):
    # # Serialize and Publish
    # transferSlice = statistic_df_ls[i].to_dict(orient='records')
    # mycol.insert_many(transferSlice)

    transferSlice = statistic_df_ls[i].to_json(orient='records', double_precision=15)
    redo = json.loads(transferSlice)
    document = {
        "data": redo
    }
    mycol.insert_one(document)
# 닫기, 시간 측정 마무리
print(" endtime:", time.time() - start)












