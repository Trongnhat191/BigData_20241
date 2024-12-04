# consumer2.py
from kafka import KafkaConsumer
import json

# Khởi tạo consumer
consumer = KafkaConsumer(
    "example_topic",
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    group_id="group2",
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def receive_data():
    try:
        print("Consumer 2 started - Processing all messages")
        for message in consumer:
            data = message.value
            print(f"Consumer 2 received: {data}")
    except KeyboardInterrupt:
        consumer.close()
        print("\nConsumer 2 stopped")

if __name__ == "__main__":
    receive_data()