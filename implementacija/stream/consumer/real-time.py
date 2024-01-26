from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, FloatType, LongType, IntegerType
from pyspark.sql.window import Window

conf = SparkConf().setAppName(
    "stream-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(
    conf=conf).enableHiveSupport().getOrCreate()

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

topics = "btc-usdt,eth-usdt,bnb-usdt"
data = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:19092") \
            .option("subscribe", topics) \
            .load()

df = data.select(F.from_json(F.col("value").cast(
    "string"), schema).alias("data"), "topic")
df = df.select("data.*", "topic")
df = df.withColumn("open_time", (F.col("open_time") / 1000).cast("timestamp"))
df = df.withColumnRenamed("topic", "pairid")
df = df.withWatermark("open_time", "10 minutes")

# Bollinger bands and Moving Average
window_spec = F.window("open_time", "5 minutes")
bollinger_df = df.groupBy(window_spec, "pairid").agg(
    F.avg("close").alias("moving_avg_close"),
    F.stddev("close").alias("stddev_close"),
    F.avg("num_trades").alias("avg_num_trades"),
    F.avg("volume").alias("avg_volume"),
    F.last("close").alias("last_close")
).withColumn(
    "upper_band", F.col("moving_avg_close") + (F.col("stddev_close") * 2)
).withColumn(
    "lower_band", F.col("moving_avg_close") - (F.col("stddev_close") * 2)
).withColumn(
    "trend",
    F.when(
        F.col("last_close") > F.col("moving_avg_close"), "Increasing"
    ).when(
        F.col("last_close") < F.col("moving_avg_close"), "Decreasing"
    ).otherwise("Steady")
).select(
    F.col("window.start").alias("start"),
    F.col("window.end").alias("end"),
    "pairid",
    "avg_num_trades",
    "avg_volume",
    "moving_avg_close",
    "upper_band",
    "lower_band",
    "trend"
)

final_df = df.groupBy(window_spec).agg(
    F.sum("quote_asset_volume").alias("total_quote_asset_volume"),
    F.avg("quote_asset_volume").alias("avg_quote_asset_volume")
).select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    "total_quote_asset_volume",
    "avg_quote_asset_volume"
)


def write_bollinger_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("real_time_statistics_fix", mode="append")


def write_total_volume_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("real_time_volume_stats", mode="append")


query1 = bollinger_df.writeStream.outputMode("update").trigger(
    processingTime='5 minutes').foreachBatch(write_bollinger_to_hive).start()
query2 = final_df.writeStream.outputMode("update").trigger(
    processingTime='5 minutes').foreachBatch(write_total_volume_to_hive).start()

query1.awaitTermination()
query2.awaitTermination()
