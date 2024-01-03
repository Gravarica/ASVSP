import os 
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, avg
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, TimestampType

print("Spark job started")

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("app1").setMaster("spark://localhost:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf) \
                            .enableHiveSupport() \
                            .getOrCreate()

print("Trying to read from file..." + HDFS_NAMENODE)

btc_usdt_df = spark.read.parquet(HDFS_NAMENODE + "/test/BTC-USDT.parquet")

print("Picked up data frame...")

btc_usdt_df.write.mode("overwrite").saveAsTable("tabela")

print("Saved to table!")