# -*- coding: utf-8 -*-
"""
Created on 2025-12-01 12:57:59

@author: Kate Yang
@description: This script is used for fetch data from AKshare and send to kafka broker
"""
import time
import os
import logging
import json
import akshare as ak
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# 配置日志
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO')
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# 设置 Kafka 相关日志为 WARNING 级别
logging.getLogger('kafka').setLevel(LOGGING_LEVEL)

# 常量
STOCK_SYMBOLS_STR = os.getenv('STOCK_SYMBOLS') # 示例是中国A股的股票代码
STOCK_SYMBOLS = STOCK_SYMBOLS_STR.split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_SERVER = os.getenv('KAFKA_SERVER')
MAX_RETRIES = int(os.getenv('KAFKA_MAX_RETRIES'))
RETRY_DELAY = int(os.getenv('KAFKA_RETRY_DELAY'))


# 初始化 Kafka producer
def create_producer(server):
    return KafkaProducer(
        bootstrap_servers=[server],
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )


for attempt in range(MAX_RETRIES):
    try:
        logging.info(f"Attempting to connect to Kafka broker: {KAFKA_SERVER}, Attempt: {attempt + 1}")
        producer = create_producer(KAFKA_SERVER)
        logging.info("Successfully connected to Kafka broker.")
        break  # Exit the loop on successful connection
    except NoBrokersAvailable as e:
        logging.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
        if attempt < MAX_RETRIES - 1:
            logging.info(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
        else:
            logging.error("Exhausted all retries. Failed to connect to Kafka broker.")
            raise


# 使用akshare获取A股股票实时数据
def fetch_stock_data(symbol):
    """
    Fetch stock data from the AKShare API (A股市场数据).
    """
    try:
        # 获取A股实时数据
        df = ak.stock_zh_a_spot()
        df.drop(["涨跌额", "涨跌幅", "买入", "卖出"], axis=1, inplace=True)

        df.rename(columns={
            "名称": "名称",
            "最新价": "收盘价",
            "最高": "最高价",
            "最低": "最低价",
            "今开": "开盘价",
            "昨收": "前收盘价"
        }, inplace=True)

        df = df[["代码", "开盘价", "最高价", "最低价",
                 "收盘价", "前收盘价", "成交量", "成交额"]]

        stock_data = df[df['代码'].isin(symbol)]
        # stock_data = ak.stock_individual_basic_info_xq(symbol="SH601127")
        # 如果找不到股票数据，则返回None
        if stock_data.empty:
            return None
        return stock_data.to_dict(orient='records')[0] # 转换为字典形式返回
    except Exception as err:
        logging.error(f"An error occurred - in fetch real time: {err}")
        return None


# 持续抓取并发送数据到Kafka
while True:
    logging.info("Current environment:", dict(os.environ))


    stock_data = fetch_stock_data(STOCK_SYMBOLS)

    if stock_data:
        try:
            # json_str = json.dumps(stock_data, ensure_ascii=False)
            # producer.send(KAFKA_TOPIC, value=json_str.encode('utf-8'))
            producer.send(KAFKA_TOPIC, value=stock_data)
            logging.info(f"Fetched and sent data: {stock_data}")
        except Exception as e:
            logging.error(f"An error occurred while sending data to Kafka: {e}")

    # 每60秒抓取一次数据
    time.sleep(60)

