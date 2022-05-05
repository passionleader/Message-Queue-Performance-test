# Mongo로부터 꺼내기
# 형태 : 1doc : 12800row 구조 꺼내는 형식
# 램을 과도하게 사용하지 않도록 유의하면서 코드 짤 것
# 데이터프레임으로 변환 성공

import time
import pandas as pd
from pandas import DataFrame
import pymongo

import json
import pyarrow as pa  # parquet 처리용
import pyarrow.parquet as pq

start = time.time()

myclient = pymongo.MongoClient("mongodb://114.70.212.154:27017/")
mydb = myclient["TDMS"]
mycol = mydb["experiment1"]
print("Mongo 연결 시간:", time.time() - start)

# 방법 1: 반복문으로 참조
# array = mycol.find({"type": "CNC"}, {"_id": 0, "data": 1}).sort("init_time", pymongo.ASCENDING)
# for value in array:
#   a = DataFrame(value)

# 방법 2: list 함수 사용
cursor = mycol.find({
    "group": "DAQ",
    "second": {
        "$gte": 1564113193,  # min
        "$lt": 1564113222  # max
    }
}, {
    "_id": 0,
    "data": 1
})
# 설명: type 속성이 CNC 데이터를 찾기, _id 속성 죽이고 data 속성 살리기, init_time 오름차순으로 정렬하기(순서대로)
# 주의: 수정 될 만한 여지가 있는 것: CNC->NC, CNC_TIME->(스키마맵에 따라 변경)
print("검색 조건 요청까지 걸리는 시간:", time.time() - start)


result_list = list(cursor)
print("리스트로 변환 시간:", time.time() - start)

# ASCENDING 작동 확인 - 정렬 문제 없음
# test = list(mycol.find({}, {"_id": 0, "init_time": 1}).sort("init_time", pymongo.ASCENDING))
# print(test)

# document 내의 12800row 한 dict 묶음인데, 이를 풀어주기 / DataFrame 저장
# [한 배치, 최대 doc개수][data][doc 내의 행 개수, 최대 12800]
# DataFrame(arr[i]["data"][1row])


# 반복문을 통해 데이터프레임으로 바꾸기
dataset = DataFrame()
for i in range(len(result_list)):
    temp_df = DataFrame(result_list[i]["data"])
    dataset = pd.concat([dataset, temp_df], ignore_index=True)
    # dataset = dataset.append(temp_df, ignore_index=True)  # slower
    del[temp_df]  # flush
print("DataFrame 변환까지 누적시간:", time.time() - start)

# concat 사용시 순서 유지되는지 확인하기 -  정렬 문제 없음
# print(dataset["TimingNC"][0],dataset["TimingNC"][10000],dataset["TimingNC"][20000],dataset["TimingNC"][30000],dataset["TimingNC"][40000])
# print(dataset)

# 최종 결과
print(dataset)

# parquet 파일 저장
# Table = pa.Table.from_pandas(dataset)
# pq.write_table(Table, "filename" + '+' + "hello" + '.parquet')
# print("parquet으로 저장까지 누적:", time.time() - start)


# 100 sec data - 1279999 rows - Find in 21.280s


# ## # ## # ## # ## # ## # ## # ## # ## # ## # ## # ## # ## # #









"""
2.0.4 Version DB to Dataframe (RANGE)
https://pypi.org/project/influxdb-client/
"""
# various import
from time import time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions

# Authorize
# You need to get Authorize TOKEN(at serverIP:8086) and make BUCKET before
token = "uWEUQi6A4jvuKrgktEZHaBENiUO2uAFXbwmG7kg8ZwFMKgFTTpD8PXnWbY6BDBLUYt0jTM5bxxiJDco_0jyyKw=="
org = "kitech"  # organization
client = InfluxDBClient(url="http://114.70.212.154:8086", token=token, org=org, debug=False)

start = time()  # 수행 시간 측정용

bucket = "TDMS"  # bucket(Database) Name
measurement_name = "experiment1"  # experiment name
search_from, search_to = 1564113193, 1564113222,
aggregate_cycle = "1000ms"  # ms, s, m, h, d, mo, y
aggregate_func = "skew"  # Descriptive function
# unique, min, max, median, mean, sum, first, last, count
# unique, sort, skew, integral, mode, spread, stddev
# derivative, nonnegative derivative, distinct, increase

query_raw = 'from(bucket: "TDMS")' \
        '|> range(start: {}, stop: {})' \
        '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
        '|> filter(fn: (r) => r["group"] == "DAQ")' \
        '|> drop(columns: ["_start", "_stop", "_measurement", "second"])' \
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' \
        '|> yield(name: "data_name")' \
        ''.format(search_from, search_to)

query_statistic = 'from(bucket: "TDMS")' \
        '|> range(start: {}, stop: {})' \
        '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
        '|> filter(fn: (r) => r["group"] == "DAQ")' \
        '|> aggregateWindow(every: {}, fn: {}, createEmpty: false)' \
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'\
        '|> yield(name: "data_name")' \
        ''.format(search_from, search_to, aggregate_cycle, aggregate_func)

query_result_1 = client.query_api().query_data_frame(org=org, query=query_statistic)
show_result_1 = query_result_1.drop(["result", "table"], axis=1)
print(show_result_1.columns)
print(show_result_1)

# query_result_2 = client.query_api().query_data_frame(org=org, query=query_statistic)
# show_result_2 = query_result_2.drop(["result", "table", "_start", "_stop", "_measurement", "second", "group"], axis=1)
# print(show_result_2)

# 속성 개요
# Query Result Columns
# result : query result name
# table : ??
# _time : real timestamp
# _start : query start time
# _stop : query end time
# _measurement : experiment name
# group : DAQ, CNC, Sensor
# second : int(timestamp) - based on 'second'
# Others.. : dataframe's column

# Close Client
client.__del__()
print("res:", time() - start)