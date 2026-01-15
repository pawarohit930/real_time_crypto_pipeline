# Databricks notebook source
silver_df = spark.readStream.table("silver_coin.crypto_prices")

from pyspark.sql.functions import *
from pyspark.sql.window import Window
gold_agg = (
    silver_df.groupBy("coin_id", "symbol", window("event_time", "5 minutes"))
.agg(
    avg("avg_price").alias("avg_price"),
    max("price_change_pct").alias("max_price_change"),
    sum("total_volume").alias("total_volume")
))

gold_flat = (
    gold_agg.
    select(
        col("coin_id"),
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_price"),
        col("max_price_change"),
        col("total_volume")
    )
)

query = (
    gold_flat
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "dbfs:/Volumes/workspace/default/checkpoints/crypto_gold"
    )
    .trigger(availableNow=True)
    .table("gold_coin.crypto_metrics")
)

from pyspark.sql.functions import *

def alert_logic(df, batch_id):
    alerts = (
        df.filter(col("price_change_pct") <= -10)
        .select(
            col("coin_id"),
            col("symbol"),
            col("event_time"),
            col("price_change_pct"),
            lit("PRICE_DROP_GT_10").alias("alert_type")
        )

    )

    if alerts.count() > 0:
        (
        alerts.write
        .format("delta")
        .mode("append")
        .saveAsTable("gold_coin.crypto_alerts")
        )
    
alert_query = (
    spark.readStream
    .table("silver_coin.crypto_prices")
    .writeStream
    .foreachBatch(alert_logic)
    .option(
        "checkpointLocation",
        "dbfs:/Volumes/workspace/default/checkpoints/crypto_alerts"
    )
    .trigger(availableNow=True)
    .start()
)
