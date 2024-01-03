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

df = spark.read.option("mergeSchema", "true").schema(customSchema).parquet(
    HDFS_NAMENODE + "/test/BTC-USDT.parquet")

df = df.withColumn("date", F.to_date("open_time"))

window = Window.partitionBy("date")

df = df.withColumn("avg_num_trades", F.avg("number_of_trades").over(window))

df = df.withColumn("avg_trade_volume", F.avg("volume").over(window))

result_df = df.select("date", "avg_num_trades", "avg_trade_volume") \
              .distinct() \
              .orderBy(F.desc("date"))

print("Picked up data frame...")

result_df.write.mode("overwrite").saveAsTable("btc_usdt_daily_averages")

print("Saved to table!")
