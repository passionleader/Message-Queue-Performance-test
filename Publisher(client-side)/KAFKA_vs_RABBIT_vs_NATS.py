# 순수 전송 성능만을 테스트 한다

# 임포트
import numpy as np
import scipy.stats
import pika  # RabbitMQ module
import pyarrow.parquet as pq  # read parquet
from kafka import KafkaProducer  # 카프카 전송하기
from tqdm import tqdm
import json
from time import time  # processing performance
import scipy.stats
import pyarrow.parquet as pq  # read parquet
import pickle
import zlib
from json import dumps
# various import
import numpy as np
import scipy.stats
import re  # 문자열 나누기 전용(filename)
import os  # load file path
from time import time  # processing performance
import pyarrow.parquet as pq  # read parquet
from tqdm import tqdm
import asyncio
import pickle
import zlib
from nats.aio.client import Client as NATS


# 실험 변수
parquet_file_path = "../dataset.parquet"

# # 래빗엠큐
# # rabbitMQ 서버 정보 선언
# server_host = '114.70.212.154'
# rabbit_account = 'admin'
# rabbit_password = '12345'
#
#
# # rabbitMQ Setting
# def connect_rabbit():
#     # set account - 서버측에서 만든 account 사용해서 접속 가능하게 함
#     credentials = pika.PlainCredentials(rabbit_account, rabbit_password)
#     # make connection to a broker on the Linux
#     connection = pika.BlockingConnection(pika.ConnectionParameters(host=server_host, credentials=credentials))
#     channel = connection.channel()
#     # before sending, make sure the recipient queue exists.(unless, rabbit will delete message)
#     channel.queue_declare(queue='parquet')
#     print("[RabbitMQ]successfully connected to RabbitMQ server")
#     return channel, connection
#
#
# # 기술 통계 계산
# def statistic_calc(batch_df):
#     statistic = batch_df.groupby('100ms').agg(
#         [np.mean, min, max, np.median, np.var, scipy.stats.skew, scipy.stats.kurtosis])
#     statistic = statistic.stack(level=0).reset_index()  # 건희는 이게 편해, 채널이 column 으로
#     return statistic
#
#
# # make connection to rabbitMQ Server
# channel, connection = connect_rabbit()
#
# # DataFrame 형태로 변환
# table = pq.read_table(parquet_file_path)
# dataset = table.to_pandas()
#
# # 기술 통계량 계산 시 그룹화 시킬  millisecond 열 추가
# dataset["100ms"] = (dataset["Timestamp"]*10).apply(int)  # 기숱롱계량 구하기 위함
# dataset = dataset.drop("Timestamp", axis=1)
#
# # 시작 입력
# input("반드시 subscriber 먼저 실행시켜주세요 > 확인:엔터")
#
# # 시간 측정
# start = time()  # 시간 측정
#
# # * 100ms 배치의 총 개수 계산 후 그만큼 반복하는 제너레이터 생성
# df_start_sec = dataset.loc[0, ['100ms']][0]//10
#
# # 기술 통계량 한 번 구한 뒤 반복 전송, 성능 측정
# statistic_df = statistic_calc(dataset.loc[dataset['100ms']//10 == df_start_sec + 50])
# print("전송중")
#
# for i in range(50000):
#     transferSlice = zlib.compress(pickle.dumps(statistic_df))
#     channel.basic_publish(exchange='', routing_key='parquet', body=transferSlice)
#
# # 마지막 알리기
# fin_alert = [{"fin": 1}]
# fin = zlib.compress(pickle.dumps(fin_alert))
# channel.basic_publish(exchange='', routing_key='parquet', body=fin)
#
# # 닫기, 시간 측정 마무리
# print(" endtime:", time() - start)
# connection.close()
#






''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


# # kafka 프로듀서 옵션 설정
# def get_producer_option():
#     # file setting 으로 부터 전송할 topic, host:port 불러온다
#     topic_name = "parquet"
#     server = "114.70.212.154:9092"
#     server2 = "114.70.212.154:9094"
#
#     # 기존에 하던 옵션들
#     producer = KafkaProducer(
#         acks=0,  # 무결성 검증: 0,1,'all'
#         compression_type='lz4',  # 압축 형태: 처리량 순으로 lz4, snappy, none(기본값)
#         bootstrap_servers=[server, server2],  # 포트
#         value_serializer=lambda x: zlib.compress(pickle.dumps(x)),  # 사용자가 제공한 메시지를 변환하는 데 사용
#         linger_ms=1,  # 추가 메시지 대기 시간
#         buffer_memory=32 * 1024 * 1024,
#         # default. 서버로 데이터를 보내기 전 대기할 수 있는 전체 메모리 바이트. 배치 전송과 같은 딜레이 발생 시 사용
#         max_request_size=30 * 1024 * 1024,  # default. 프로듀서가 보낼 수 있는 최대 메시지 바이트 사이즈
#         batch_size=32 * 1024  # 각 파티션에 하나씩 배치. 배치 크기가 작으면 일괄 처리가 작아지며 처리량이 줄어 들 수 있음, (크면 가용성 떨어짐)
#     )
#     # # 기본옵션 타협
#     # producer = KafkaProducer(
#     #     acks=0,  # 무결성 검증: 0,1,'all'
#     #     bootstrap_servers=[server],  # 포트
#     #     value_serializer=lambda x: zlib.compress(pickle.dumps(x)),  # 사용자가 제공한 메시지를 변환하는 데 사용
#     # )
#     return topic_name, producer
#
#
# # Loop through generator obj 제너레이터를 통한 반복 전송
# def slice_generator(time_range):
#     list = range(time_range)  # 범위만 담고 있음
#     for y in tqdm(list):  # 범위 만큼 반복
#         yield y  # 반복 할 것 반환
#
#
# # 기술 통계 계산
# def statistic_calc(batch_df):
#     statistic = batch_df.groupby('100ms').agg(
#         [np.mean, min, max, np.median, np.var, scipy.stats.skew, scipy.stats.kurtosis])
#     statistic = statistic.stack(level=0).reset_index()  # 건희는 이게 편해, 채널이 column 으로
#     return statistic
#
# # 토픽이름, producer 옵션 가져오기
# topic_name, producer = get_producer_option()
#
# # DataFrame 형태로 변환
# table = pq.read_table(parquet_file_path)
# dataset = table.to_pandas()
#
# # 기술 통계량 계산 시 그룹화 시킬  millisecond 열 추가
# dataset["100ms"] = (dataset["Timestamp"]*10).apply(int)  # 기숱롱계량 구하기 위함
# dataset = dataset.drop("Timestamp", axis=1)
#
# # 시작 입력
# input("반드시 subscriber 먼저 실행시켜주세요 > 확인:엔터")
#
# # 시간 측정
# start = time()  # 시간 측정
#
# df_start_sec = dataset.loc[0, ['100ms']][0]//10
# df_end_sec = dataset.loc[len(dataset) - 1, ['100ms']][0]//10
# generatorObj = slice_generator(int(df_end_sec - df_start_sec))
#
# # 기술 통계량 한 번 구한 뒤 반복 전송, 성능 측정
# statistic_df = statistic_calc(dataset.loc[dataset['100ms']//10 == df_start_sec + 50])
# print("전송중")
#
# for i in range(50000):
#     # Serialize and Publish
#     transferSlice = zlib.compress(pickle.dumps(statistic_df))
#     producer.send(topic_name, value=transferSlice)
#
# # 마지막 알리기
# fin_alert = [{"fin": 1}]
# fin = zlib.compress(pickle.dumps(fin_alert))
# producer.send(topic_name, value=fin)
#
# # 닫기, 시간 측정 마무리
# print(" endtime:", time() - start)
# producer.flush()
#
''''''''''''''''''''''''''''''''''''''''''''



# 실험 변수
parquet_file_path = "../dataset.parquet"


# NATS module Loop Function - main dataset
async def publish_main():
    nc = NATS()
    await nc.connect(servers=["nats://114.70.212.154:4222"])

    statistic_df = statistic_calc(dataset.loc[dataset['100ms'] // 10 == df_start_sec + 50])
    print("전송중")

    for i in range(50000):
        transferSlice = zlib.compress(pickle.dumps(statistic_df))
        await nc.publish("milling", transferSlice)
    await nc.close()


# Loop through generator obj 제너레이터를 통한 반복 전송
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


# Nats loop by asyncio
loop = asyncio.get_event_loop()

# DataFrame 형태로 변환
table = pq.read_table(parquet_file_path)
dataset = table.to_pandas()

# 기술 통계량 계산 시 그룹화 시킬  millisecond 열 추가
dataset["100ms"] = (dataset["Timestamp"]*10).apply(int)  # 기숱롱계량 구하기 위함
dataset = dataset.drop("Timestamp", axis=1)

# 시작 입력
input("반드시 subscriber 먼저 실행시켜주세요 > 확인:엔터")

# 시간 측정
start = time()  # 시간 측정

# 전송: 본 데이터(100ms 열을 바탕으로 0.1초 분량씩 전송)
# * 100ms 배치의 총 개수 계산 후 그만큼 반복하는 제너레이터 생성
df_start_sec = dataset.loc[0, ['100ms']][0]//10
df_end_sec = dataset.loc[len(dataset) - 1, ['100ms']][0]//10
generatorObj = slice_generator(int(df_end_sec - df_start_sec))

loop.run_until_complete(publish_main())
loop.close()

# 닫기, 시간 측정 마무리
print(" endtime:", time() - start)




# 오류 발생 중
