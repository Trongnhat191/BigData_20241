import os
import time
from kafka import KafkaProducer
from pymongo import MongoClient
import json

# Kết nối đến Kafka
kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_servers])

# Kết nối đến MongoDB
mongodb_uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/Bigdata')
client = MongoClient(mongodb_uri)
db = client.get_database()
collection = db.Bigdata

# Đọc dữ liệu từ MongoDB một lần
documents = collection.find()

# Gửi từng document đến Kafka
for doc in documents:
    # Chuyển đổi document thành JSON string
    message = json.dumps(doc, default=str).encode('utf-8')
    
    # Gửi message đến Kafka
    producer.send('your_topic', value=message)

# Đợi tất cả các message được gửi
producer.flush()

# Đóng kết nối
producer.close()
client.close()

print("Hoàn thành việc gửi dữ liệu từ MongoDB đến Kafka.")

