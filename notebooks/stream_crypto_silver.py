# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_df = (
    spark.readStream.table("bronze_coin.crypto_raw")
)
schema = ArrayType(
    StructType([
        StructField("id", StringType()), 
        StructField("symbol", StringType()),
        StructField("name", StringType()),
        StructField("current_price", DoubleType()),
        StructField("market_cap", DoubleType()),
        StructField("total_volume", DoubleType()),
    ])
)
parsed_df = (bronze_df.withColumn("json", from_json(col("raw_json"), schema))
             .withColumn("coin", explode("json")))


flat_df = (
    parsed_df.select(
        col("coin.id").alias("coin_id"),
        col("coin.symbol"),
        col("coin.name"),
        col("coin.current_price").alias("price"),
        col("coin.market_cap"),
        col("coin.total_volume").alias("volume"),
        col("ingest_time")
    )
    .withColumn("event_time", current_timestamp())

    )

watermarked_df = (
    flat_df.withWatermark("event_time", "5 minutes")
)

windowed_prices = (
    watermarked_df
    .groupBy(
        "coin_id",
        "symbol",
        window("event_time", "1 minute")
    )
    .agg(
        avg("price").alias("avg_price"),
        sum("volume").alias("total_volume")

    )
)

def process_batch(df, batch_id):

    from pyspark.sql.window import Window
    df2 = df.withColumn("event_time", col("window.start"))
    w = Window.partitionBy("coin_id").orderBy("event_time")

    final_df = (
        df2
        .withColumn("prev_avg_price", lag("avg_price").over(w))
        .withColumn(
            "price_change_pct",
            round(
                ((col("avg_price") - col("prev_avg_price")) /
                 col("prev_avg_price")) * 100, 4
            )
        )
        .drop("window")
    )

    (
        final_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("silver_coin.crypto_prices")
    )


query = (
    windowed_prices.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation",
            "dbfs:/Volumes/workspace/default/checkpoints/crypto_silver_v4")
    .trigger(availableNow=True)
    .start()
)