from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from datetime import datetime

# Khởi tạo consumer Kafka
consumer = KafkaConsumer(
    "example_topic",
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    group_id="group1",
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kết nối Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],  # Địa chỉ Elasticsearch
    verify_certs=True
)

# Hàm để chuyển đổi timestamp sang định dạng ISO 8601
def format_timestamp(timestamp):
    # Chuyển đổi về định dạng ISO 8601
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        return dt.isoformat()  # Trả về định dạng ISO 8601
    except ValueError:
        return timestamp  # Trả về nguyên gốc nếu không thể chuyển đổi

# Hàm để gửi dữ liệu vào Elasticsearch
def index_to_elasticsearch(data):
    try:
        # Chuyển đổi timestamp sang định dạng ISO 8601
        if 'timestamp' in data:
            data['timestamp'] = format_timestamp(data['timestamp'])

        # Gửi dữ liệu vào chỉ mục example_index
        es.index(index="example_index", document=data)
        print(f"Data indexed to Elasticsearch: {data}")
    except Exception as e:
        print(f"Error indexing data to Elasticsearch: {e}")

# Hàm để nhận dữ liệu từ Kafka và gửi vào Elasticsearch
def receive_data():
    try:
        print("Consumer started - Processing all messages and sending to Elasticsearch")
        for message in consumer:
            data = message.value  # Dữ liệu từ Kafka
            print(f"Received data from Kafka: {data}")
            
            # Gửi dữ liệu vào Elasticsearch
            index_to_elasticsearch(data)
            
    except KeyboardInterrupt:
        consumer.close()
        print("\nConsumer stopped")

if __name__ == "__main__":
    receive_data()
