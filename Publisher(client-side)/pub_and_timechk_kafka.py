'''
논문 실험용 코드
'''
import time  # processing performance
import pyarrow.parquet as pq  # read parquet
from kafka import KafkaProducer  # 카프카 전송하기
from orjson import dumps
from tqdm import tqdm
import json

# 실험 변수
parquet_file_path = "../dataset.parquet"
rowBatchSize = 12800


# kafka 프로듀서 옵션 설정
def get_producer_option():
    # file setting 으로 부터 전송할 topic, host:port 불러온다
    topic_name = "parquet"
    server = "114.70.212.154:9092"

    producer = KafkaProducer(
        acks=0,  # 무결성 검증: 0,1,'all'
        compression_type='lz4',  # 압축 형태: 처리량 순으로 lz4, snappy, none(기본값)
        bootstrap_servers=[server],  # 포트
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),  # 사용자가 제공한 메시지를 변환하는 데 사용
        linger_ms=0,  # 추가 메시지 대기 시간
        buffer_memory=32 * 1024 * 1024,
        # default. 서버로 데이터를 보내기 전 대기할 수 있는 전체 메모리 바이트. 배치 전송과 같은 딜레이 발생 시 사용
        max_request_size=30 * 1024 * 1024,  # default. 프로듀서가 보낼 수 있는 최대 메시지 바이트 사이즈
        batch_size=16 * 1024  # 각 파티션에 하나씩 배치. 배치 크기가 작으면 일괄 처리가 작아지며 처리량이 줄어 들 수 있음, (크면 가용성 떨어짐)
    )
    return topic_name, producer


# Loop through generator obj 제너레이터를 통한 반복 전송
def slice_generator():
    list = range(0, len(dataset), rowBatchSize)  # 범위만 담고 있음
    for y in tqdm(list):  # 범위 만큼 반복
        yield y  # 반환


# 토픽이름, producer 옵션 가져오기
topic_name, producer = get_producer_option()

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
    producer.send(topic_name, value=transferSlice)
    producer.flush()

# 마지막 알리기
fin_alert = {"fin": 1}
fin = json.dumps(fin_alert)
producer.send(topic_name, value=fin)

# 닫기, 시간 측정 마무리
print(" endtime:", time.time() - start)
producer.flush()
