# various import
import re  # 문자열 나누기 전용(filename)
import os  # load file path
from time import time  # processing performance
from orjson import dumps
import pyarrow.parquet as pq  # read parquet
import pika  # RabbitMQ module
from tqdm import tqdm
import pymongo
import sys
from time import time
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import orjson

'''기본 정보를 위해서 변수 선언하는 곳'''
# 저장될 DB명 및 Collection명 지정 : 실험 그룹명 및 실험명 적기
dbname = "TDMS"  # = bucket name(influxDB), collection name(mongoDB)
experiment_name = "experiment3"

# mongo DB Client
mongo_client = pymongo.MongoClient("mongodb://localhost:27017")

# Influx DB Client
org = "kitech"
token = "uWEUQi6A4jvuKrgktEZHaBENiUO2uAFXbwmG7kg8ZwFMKgFTTpD8PXnWbY6BDBLUYt0jTM5bxxiJDco_0jyyKw=="
influx_client = InfluxDBClient(url="114.70.212.154:8086", token=token)
influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)


# load file path and file list
def get_file_list():
	dir_path = "../../parquet/"
	file_list = os.listdir(dir_path)
	return dir_path, file_list


# Loop through generator obj 제너레이터를 통한 반복 전송
def slice_generator():
	list = range(0, len(dataset), 12800)  # 범위만 담고 있음
	for y in tqdm(list):  # 범위 만큼 반복
		yield y  # 반환


''' 객체 생성 및 함수 호출'''
# Call the file path
dir_path, file_list = get_file_list()

# parquet 하나 = 반복문 한 번.
file_number = 1  # 진행 중인 파일의 넘버

'''모든 파일에 대해 수행'''
for filename in file_list:
	print("\n[%d of %d]file in progress...." % (file_number, len(file_list)))
	start = time()  # 시간 측정

	# 파일 이름으로부터 그룹명(DAQ,CNC)과 확장자 분리
	title = re.split("[.+]", filename)
	data_group = title[-2]

	# DataFrame 형태로 변환
	parquet_file_path = dir_path + filename
	table = pq.read_table(parquet_file_path)
	dataset = table.to_pandas()

	# Influx 입력 시 필요한 태그키를 미리 데이터프레임에 넣어 두기: group_name, second
	# dataset.insert(0, "exp_name", dbname, allow_duplicates=False)
	# dataset.insert(0, "group", data_group, allow_duplicates=False)
	# dataset.insert(0, "second", dataset["Timestamp"].apply(int), allow_duplicates=False)
	dataset["group"] = data_group
	dataset["second"] = dataset["Timestamp"].apply(int)

	# 다음을 메타데이터로써 보내기(실험그룹명(버킷), 실험명, 현재파일의행수, 현재진행파일번호, 총파일갯수)을 딕셔너리 형태로 저장
	my_db = mongo_client[dbname]
	my_col = my_db[experiment_name]

	generatorObj = slice_generator()

	# # influx 입력 장소
	# for i in generatorObj:
	# 	newdataset = dataset[i:i + 12800]
	# 	newdataset["Timestamp"] = (newdataset["Timestamp"] * (10**9)).apply(int)
	# 	newdataset.set_index('Timestamp', inplace=True)
	# 	influx_write_api.write(
	# 		bucket = dbname, org=org,
	# 		data_frame_measurement_name = experiment_name,
	# 		data_frame_tag_columns = ['second', 'group'],
	# 		record = newdataset
	# 	)
	#
	for i in generatorObj:
		transferSlice = dataset[i:i + 12800].to_json(orient='records', double_precision=15)
		transferSlice = orjson.loads(transferSlice)
		#print(transferSlice[0]["second"])
		document = {
			"second" : transferSlice[0]["second"],
			"group" : data_group,
			"data" : transferSlice
		}
		my_col.insert_one(document)