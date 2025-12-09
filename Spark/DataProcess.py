#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Kate Yang
@description: This script is used for ...
"""
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, unix_timestamp, from_unixtime, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# -------------------------------
# 1. 日志配置
# -------------------------------
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO')
logging.basicConfig(level=LOGGING_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# -------------------------------
# 2. MySQL 配置
# -------------------------------
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'stockdb')
MYSQL_USER = os.getenv('MYSQL_USER', 'appuser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '123456a')

# -------------------------------
# 3. Kafka 配置
# -------------------------------
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock_prices')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')

# # -------------------------------
# # 4. 初始化 SparkSession
# # -------------------------------
spark = SparkSession.builder \
    .appName("AkShareStockProcessor") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
#
# # -------------------------------
# # 5. 定义 Kafka 消息 schema
# # -------------------------------
schema = StructType([
    StructField("代码", StringType()),
    StructField("开盘价", DoubleType()),
    StructField("最高价", DoubleType()),
    StructField("最低价", DoubleType()),
    StructField("收盘价", DoubleType()),
    StructField("前收盘价", DoubleType()),
    StructField("成交量", DoubleType()),
    StructField("成交额", DoubleType()),
])

#
# # -------------------------------
# # 6. 读取 Kafka 流
# # -------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# -------------------------------
# 7. 解析 JSON 并重命名字段
# -------------------------------
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_parsed = df_parsed \
    .withColumnRenamed("代码", "code") \
    .withColumnRenamed("开盘价", "open_price") \
    .withColumnRenamed("最高价", "high_price") \
    .withColumnRenamed("最低价", "low_price") \
    .withColumnRenamed("收盘价", "close_price") \
    .withColumnRenamed("前收盘价", "prev_close_price") \
    .withColumnRenamed("成交量", "volume") \
    .withColumnRenamed("成交额", "turnover")

# -------------------------------
# 8. 写入 MySQL 的函数
# -------------------------------

def write_to_mysql(df, batch_id):
    try:
        count = df.count()
        logging.info(f"Batch {batch_id}: {count} rows")
        if count == 0:
            return
        df.write.format("jdbc") \
            .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}") \
            .option("dbtable", 'stock_quotes') \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASSWORD) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        logging.error(f"Error writing batch {batch_id} to MySQL: {e}")

# -------------------------------
# 9. 启动流处理
# -------------------------------
sqlquery = df_parsed.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint_akshare") \
    .start()

console_query = df_parsed.writeStream\
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false")\
    .option("numRows", 10) \
    .start()


sqlquery.awaitTermination()
console_query.awaitTermination()





