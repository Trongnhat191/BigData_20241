from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
from datetime import datetime
import json
from kafka import KafkaConsumer

schema = StructType([
    StructField("zpid", StringType(), True),
    # ... (previous schema remains the same)
    StructField("videoCount", IntegerType(), True)
])

def create_spark_session():
    """Create a Spark Session with Kafka integration."""
    return SparkSession \
        .builder \
        .appName("KafkaSparkStructured") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

def create_elasticsearch_client(hosts=None):
    """
    Create Elasticsearch client with error handling.
    
    :param hosts: List of Elasticsearch hosts
    :return: Elasticsearch client
    """
    if hosts is None:
        hosts = ["http://localhost:9200"]
    
    try:
        es = Elasticsearch(
            hosts=hosts,
            verify_certs=True
        )
        # Verify connection
        if not es.ping():
            raise ValueError("Elasticsearch connection failed")
        return es
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        return None

def format_timestamp(timestamp):
    """
    Convert timestamp to ISO 8601 format.
    
    :param timestamp: Input timestamp
    :return: Formatted timestamp
    """
    try:
        if isinstance(timestamp, str):
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            return dt.isoformat()
        return timestamp
    except (ValueError, TypeError):
        return timestamp

def index_to_elasticsearch(es_client, index_name, data):
    """
    Index data to Elasticsearch with robust error handling.
    
    :param es_client: Elasticsearch client
    :param index_name: Target index name
    :param data: Data to be indexed
    """
    if not es_client:
        print("Elasticsearch client not initialized")
        return

    try:
        # Ensure data is a dictionary
        if not isinstance(data, dict):
            data = json.loads(data)

        # Convert timestamp if present
        if 'timestamp' in data:
            data['timestamp'] = format_timestamp(data['timestamp'])

        # Index the document
        es_client.index(index=index_name, document=data)
        print(f"Successfully indexed data to {index_name}")
    except Exception as e:
        print(f"Error indexing data to Elasticsearch: {e}")

def create_kafka_consumer(bootstrap_servers, topic):
    """
    Create a Kafka consumer with error handling.
    
    :param bootstrap_servers: List of Kafka bootstrap servers
    :param topic: Kafka topic to consume from
    :return: Kafka consumer
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='spark-elasticsearch-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        print(f"Error creating Kafka consumer: {e}")
        return None

def main():
    """
    Main function to orchestrate Kafka consumption and Elasticsearch indexing.
    """
    # Kafka and Elasticsearch configuration
    kafka_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
    kafka_topic = 'example_topic'
    es_hosts = ['http://localhost:9200']
    es_index = 'example_index'

    # Create Elasticsearch client
    es_client = create_elasticsearch_client(es_hosts)
    
    # Create Kafka consumer
    kafka_consumer = create_kafka_consumer(kafka_servers, kafka_topic)
    
    if not kafka_consumer:
        print("Failed to create Kafka consumer. Exiting.")
        return

    try:
        for message in kafka_consumer:
            try:
                # Process and index each message
                index_to_elasticsearch(es_client, es_index, message.value)
            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    finally:
        if kafka_consumer:
            kafka_consumer.close()

if __name__ == "__main__":
    main()