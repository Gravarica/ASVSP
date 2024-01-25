from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, FloatType, LongType, IntegerType

conf = SparkConf().setAppName("stream-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

schema = StructType([
    StructField("open_time", LongType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True),
    StructField("quote_asset_volume", FloatType(), True),
    StructField("num_trades", IntegerType(), True),
    StructField("taker_base_asset_volume", FloatType(), True),
    StructField("taker_quote_asset_volume", FloatType(), True),
])

# Subscribe to multiple topics
topics = "btc-usdt,eth-usdt,bnb-usdt"
data = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:19092") \
            .option("subscribe", topics) \
            .option("kafka.group.id", "group1") \
            .load()

df = data.select(F.from_json(F.col("value").cast("string"), schema).alias("data"), "topic")
df = df.select("data.*", "topic")
df = df.withColumn("open_time", (F.col("open_time") / 1000).cast("timestamp"))
df = df.withColumnRenamed("topic", "pairid")

window_spec = F.window("open_time", "5 minutes")
stats_df = df.groupBy(window_spec, "pairid").agg(
    F.avg("close").alias("moving_avg_close"),
    F.stddev("close").alias("stddev_close")
).select(
    F.col("window.start").alias("start"),
    F.col("window.end").alias("end"),
    "moving_avg_close",
    "stddev_close",
    "pairid"
)

bollinger_df = stats_df.withColumn(
    "upper_band",
    F.col("moving_avg_close") + (F.col("stddev_close") * 2)
).withColumn(
    "lower_band",
    F.col("moving_avg_close") - (F.col("stddev_close") * 2)
).select(
    "start",
    "end",
    "pairid",
    "moving_avg_close",
    "upper_band",
    "lower_band"
)

def write_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("real_time_statistics", mode="append")

query = bollinger_df.writeStream.outputMode("update").trigger(processingTime='5 minutes').foreachBatch(write_to_hive).start()

query.awaitTermination()
