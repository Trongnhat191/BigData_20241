import json
from kafka import KafkaConsumer
from hdfs import InsecureClient
import os
from datetime import datetime

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
KAFKA_TOPIC = 'example_topic'
KAFKA_GROUP_ID = 'group2'

# HDFS configuration
HDFS_HOST = 'http://localhost:9870'
HDFS_USER = 'root'
HDFS_BASE_PATH = '/data/kafka_messages'

def setup_hdfs_client():
    """
    Create an HDFS client for file operations
    """
    return InsecureClient(HDFS_HOST, user=HDFS_USER)

def create_hdfs_path(hdfs_client, date_path):
    """
    Create HDFS directory if it doesn't exist
    """
    try:
        hdfs_client.makedirs(date_path)
    except Exception as e:
        print(f"Error creating HDFS directory: {e}")

def save_to_hdfs(hdfs_client, message, base_path):
    """
    Save Kafka message to HDFS
    
    Args:
    - hdfs_client: HDFS client
    - message: Kafka message to save
    - base_path: Base HDFS path for storage
    """
    # Create timestamp-based path
    current_time = datetime.now()
    date_path = os.path.join(base_path, 
                              current_time.strftime('%Y'), 
                              current_time.strftime('%m'), 
                              current_time.strftime('%d'))
    
    # Ensure HDFS path exists
    create_hdfs_path(hdfs_client, date_path)
    
    # Generate unique filename
    filename = f"{current_time.strftime('%H_%M_%S_%f')}.json"
    full_path = os.path.join(date_path, filename)
    
    try:
        with hdfs_client.write(full_path, encoding='utf-8') as writer:
            json.dump(message, writer)
        print(f"Message saved to {full_path}")
    except Exception as e:
        print(f"Error saving message to HDFS: {e}")

def kafka_hdfs_consumer():
    """
    Main consumer function to read from Kafka and save to HDFS
    """
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Initialize HDFS Client
    hdfs_client = setup_hdfs_client()
    
    # Consume messages
    try:
        for message in consumer:
            if message is not None:
                save_to_hdfs(hdfs_client, message.value, HDFS_BASE_PATH)
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_hdfs_consumer()