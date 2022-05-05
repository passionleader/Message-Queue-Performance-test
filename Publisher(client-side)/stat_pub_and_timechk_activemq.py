import numpy as np
import scipy.stats
import pickle
import zlib
import os
import time  # processing performance
import pyarrow.parquet as pq  # read parquet
from tqdm import tqdm
import stomp


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


def byte_pub(df):
    res = zlib.compress(pickle.dumps(df))
    return res


def zlib_pub(df):
    res = zlib.compress(df.to_json(orient='records', double_precision=15))
    return res


def pickle_pub(df):
    res = pickle.dumps(df)
    return res


# 실험 변수
parquet_file_path = "../dataset.parquet"

# active MQ 서버 정보 선언
user = os.getenv("ACTIVEMQ_USER") or "admin"
password = os.getenv("ACTIVEMQ_PASSWORD") or "password"
host = os.getenv("ACTIVEMQ_HOST") or "114.70.212.154"
port = os.getenv("ACTIVEMQ_PORT") or 61613
destination = "/topic/parquet"  # 토픽 세팅

# make connection to activeMQ Server
conn = stomp.Connection(host_and_ports=[(host, port)])
conn.connect(login=user, passcode=password)

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

# 500 초짜리 기술통계 데이터 만들기
statistic_df_ls = []
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

# 1초 데이터마다 보내기. 기술 통계량은 0.1초마다 구함
for i in range(len(statistic_df_ls)):
    # Serialize and Publish
    transferSlice = json_pub(statistic_df_ls[i])
    conn.send(destination, transferSlice, persistent='false')

# 닫기, 시간 측정 마무리
conn.send(destination, "SHUTDOWN", persistent='true')
print(" endtime:", time.time() - start)
conn.disconnect()
