import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, TimestampType

print("Spark job started")

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("app1").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf) \
                            .enableHiveSupport() \
                            .getOrCreate()

print("Trying to read from file..." + HDFS_NAMENODE)

customSchema = StructType([
    StructField("open_time", TimestampType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True),
    StructField("quote_asset_volume", FloatType(), True),
    StructField("number_of_trades", IntegerType(), True),
    StructField("taker_buy_base_asset_volume", FloatType(), True),
    StructField("taker_buy_quote_asset_volume", FloatType(), True),
])

crypto_pairs = ["BTC-USDT", "ETH-USDT", "BNB-USDT", "ADA-USDT", "XRP-USDT"]

consolidatedDf = None

for pair in crypto_pairs:
    file_path = f"/data/{pair}.parquet"
    df_temp = spark.read.option("mergeSchema", "true").schema(
        customSchema).parquet(HDFS_NAMENODE + file_path)
    df_temp = df_temp.withColumn("PairID", F.lit(pair))

    if consolidatedDf is None:
        consolidatedDf = df_temp
    else:
        consolidatedDf = consolidatedDf.union(df_temp)

df = consolidatedDf.withColumn("date", F.to_date("open_time"))

window = Window.partitionBy("date", "PairID")

df = df.withColumn("avg_num_trades", F.avg(
    "number_of_trades").over(window))

df = df.withColumn("total_num_trades", F.sum(
    "number_of_trades").over(window))

df = df.withColumn("avg_trade_volume", F.avg(
    "volume").over(window))

df = df.withColumn("total_trade_volume", F.sum(
    "volume").over(window))

result_df = df.select("date", "pairid", "total_num_trades", "avg_num_trades",  "total_trade_volume", "avg_trade_volume") \
              .distinct() \
              .orderBy(F.desc("date"))

print("Picked up data frame...")

result_df.write.mode("overwrite").saveAsTable("btc_usdt_daily_averages")

print("Saved to table!")
