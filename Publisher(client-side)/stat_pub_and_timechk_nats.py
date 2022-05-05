
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


# NATS module Loop Function - main dataset
async def publish_main():
    nc = NATS()
    await nc.connect(servers=["nats://114.70.212.154:4222"])
    # 1초 데이터마다 보내기. 기술 통계량은 0.1초마다 구함
    for i in range(len(statistic_df_ls)):
        # Serialize and Publish
        transferSlice = byte_pub(statistic_df_ls[i])
        await nc.publish("milling", transferSlice)
    await nc.close()


# NATS module Loop Function - metadata
async def finish(data):
    nc = NATS()
    # print("current max_payload byte : ", nc.max_payload)
    await nc.connect(servers=["nats://114.70.212.154:4222"])
    await nc.publish("milling", data)
    await nc.close()



# 실험 변수
parquet_file_path = "../dataset.parquet"

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

loop.run_until_complete(publish_main())


# 마지막 알리기
fin_alert = [{"fin": 1}]
#fin = json.dumps(fin_alert)
fin = zlib.compress(pickle.dumps(fin_alert))
loop.run_until_complete(finish(fin))

# 닫기, 시간 측정 마무리
print(" endtime:", time.time() - start)






