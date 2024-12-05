# kafka_spark_structured.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from elasticsearch import Elasticsearch

# Định nghĩa schema cho dữ liệu
# schema = StructType([
#     StructField("timestamp", StringType( ), True),
#     StructField("temperature", DoubleType(), True),
#     StructField("humidity", DoubleType(), True),
#     StructField("sensor_id", IntegerType(), True)
# ])
schema = StructType([
    StructField("zpid", StringType(), True),
    StructField("homeStatus", StringType(), True),
    StructField("detailUrl", StringType(), True),
    StructField("address", StringType(), True),
    StructField("streetAddress", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("homeType", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("zestimate", DoubleType(), True),
    StructField("rentZestimate", DoubleType(), True),
    StructField("taxAssessedValue", DoubleType(), True),
    StructField("lotAreaValue", DoubleType(), True),
    StructField("lotAreaUnit", StringType(), True),
    StructField("bathrooms", IntegerType(), True),
    StructField("bedrooms", IntegerType(), True),
    StructField("livingArea", DoubleType(), True),
    StructField("daysOnZillow", IntegerType(), True),
    StructField("isFeatured", BooleanType(), True),
    StructField("isPreforeclosureAuction", BooleanType(), True),
    StructField("timeOnZillow", IntegerType(), True),
    StructField("isNonOwnerOccupied", BooleanType(), True),
    StructField("isPremierBuilder", BooleanType(), True),
    StructField("isZillowOwned", BooleanType(), True),
    StructField("isShowcaseListing", BooleanType(), True),
    StructField("imgSrc", StringType(), True),
    StructField("hasImage", BooleanType(), True),
    StructField("brokerName", StringType(), True),
    StructField("listingSubType.is_FSBA", BooleanType(), True),
    StructField("priceChange", DoubleType(), True),
    StructField("datePriceChanged", LongType(), True),
    StructField("openHouse", StringType(), True),
    StructField("priceReduction", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("listingSubType.is_openHouse", BooleanType(), True),
    StructField("newConstructionType", StringType(), True),
    StructField("listingSubType.is_newHome", BooleanType(), True),
    StructField("videoCount", IntegerType(), True)
])
# Khởi tạo Spark Session
spark = SparkSession \
    .builder \
    .appName("KafkaSparkStructured") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
    .option("subscribe", "example_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi dữ liệu từ Kafka thành dữ liệu cấu trúc
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

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
        print("\nConsumer stopped")
# Hiển thị dữ liệu
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
