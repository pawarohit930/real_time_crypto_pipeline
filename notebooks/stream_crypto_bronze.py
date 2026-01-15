# Databricks notebook source
import requests
import time
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1
}
while True:
    try:
        response = requests.get(URL, params=PARAMS, timeout=10)
        response.raise_for_status()
        raw_payload = response.text

        df = spark.createDataFrame([Row(raw_json=raw_payload)]).withColumn("ingest_time", current_timestamp())

        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable("bronze_coin.crypto_raw")
        )
        print("Ingested batch at", time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        print("Error:", e)
    time.sleep(30) #poll every 30 seconds