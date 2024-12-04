# kafka_spark_structured.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# Định nghĩa schema cho dữ liệu
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("sensor_id", IntegerType(), True)
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

# Hiển thị dữ liệu
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
