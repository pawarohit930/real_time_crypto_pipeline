-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS bronze_coin;
CREATE TABLE IF NOT EXISTS bronze_coin.crypto_raw(
  raw_json STRING,
  ingest_time TIMESTAMP
)
USING DELTA;

select * from bronze_coin.crypto_raw
order by ingest_time desc
limit 10;


--CREATE SCHEMA IF NOT EXISTS silver_coin;
CREATE TABLE silver_coin.crypto_prices (
  coin_id STRING,
  symbol STRING,
  event_time TIMESTAMP,
  avg_price DOUBLE,
  prev_avg_price DOUBLE,
  price_change_pct DOUBLE,
  total_volume DOUBLE
)
USING DELTA
PARTITIONED BY (symbol);


CREATE SCHEMA IF NOT EXISTS gold_coin;
CREATE TABLE IF NOT EXISTS gold_coin.crypto_metrics(
  coin_id STRING,
  symbol STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  avg_price DOUBLE,
  max_price_change DOUBLE,
  total_volume DOUBLE
)
USING DELTA
PARTITIONED BY (symbol);

CREATE VOLUME IF NOT EXISTS workspace.default.checkpoints;


CREATE TABLE IF NOT EXISTS gold_coin.crypto_alerts(
coin_id STRING,
symbol STRING,
event_time TIMESTAMP,
price_change_pct DOUBLE,
alert_type STRING
)
USING DELTA;