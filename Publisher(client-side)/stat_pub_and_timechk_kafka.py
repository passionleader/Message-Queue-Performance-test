import numpy as np
import scipy.stats
import pika  # RabbitMQ module
import pickle
import zlib
import time  # processing performance
import pyarrow.parquet as pq  # read parquet
from kafka import KafkaProducer  # 카프카 전송하기
from tqdm import tqdm
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


def byte_pub(df):
    res = zlib.compress(pickle.dumps(df))
    return res


def zlib_pub(df):
    res = zlib.compress(df.to_json(orient='records', double_precision=15))
    return res


def pickle_pub(df):
    res = pickle.dumps(df)
    return res


# kafka 프로듀서 옵션 설정
def get_producer_option():
    # file setting 으로 부터 전송할 topic, host:port 불러온다
    topic_name = "parquet"
    server = "114.70.212.154:9092"

    producer = KafkaProducer(
        acks=0,  # 무결성 검증: 0,1,'all'
        compression_type='lz4',  # 압축 형태: 처리량 순으로 lz4, snappy, none(기본값)
        bootstrap_servers=[server],  # 포트
        value_serializer=lambda x: zlib.compress(pickle.dumps(x)),  # 사용자가 제공한 메시지를 변환하는 데 사용
        linger_ms=0,  # 추가 메시지 대기 시간
        buffer_memory=32 * 1024 * 1024,
        # default. 서버로 데이터를 보내기 전 대기할 수 있는 전체 메모리 바이트. 배치 전송과 같은 딜레이 발생 시 사용
        max_request_size=30 * 1024 * 1024,  # default. 프로듀서가 보낼 수 있는 최대 메시지 바이트 사이즈
        batch_size=16 * 1024  # 각 파티션에 하나씩 배치. 배치 크기가 작으면 일괄 처리가 작아지며 처리량이 줄어 들 수 있음, (크면 가용성 떨어짐)
    )
    return topic_name, producer


# 실험 변수
parquet_file_path = "../dataset.parquet"


# 토픽이름, producer 옵션 가져오기
topic_name, producer = get_producer_option()


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

# 1초 데이터마다 보내기. 기술 통계량은 0.1초마다 구함
for i in range(len(statistic_df_ls)):
    # Serialize and Publish
    transferSlice = json_pub(statistic_df_ls[i])
    producer.send(topic_name, value=transferSlice)

# 마지막 알리기
fin_alert = [{"fin": 1}]
fin = json.dumps(fin_alert)
# fin = zlib.compress(pickle.dumps(fin_alert))
producer.send(topic_name, value=fin)


# 닫기, 시간 측정 마무리
print(" endtime:", time.time() - start)
producer.flush()










