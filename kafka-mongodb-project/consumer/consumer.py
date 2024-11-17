import os
import time
from kafka import KafkaConsumer

# Kết nối đến Kafka
kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Đợi Kafka khởi động
#time.sleep(30)

consumer = KafkaConsumer('your_topic', bootstrap_servers=[kafka_bootstrap_servers], auto_offset_reset='earliest')

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")

