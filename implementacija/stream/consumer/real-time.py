from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, FloatType, LongType, IntegerType
from pyspark.sql.window import Window

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
df = df.withWatermark("open_time", "10 minutes")

# Bollinger bands and Moving Average
window_spec = F.window("open_time", "5 minutes")
bollinger_df = df.groupBy(window_spec, "pairid").agg(
    F.avg("close").alias("moving_avg_close"),
    F.stddev("close").alias("stddev_close"),
    F.avg("num_trades").alias("avg_num_trades"),
    F.avg("volume").alias("avg_volume")
).withColumn(
    "upper_band", F.col("moving_avg_close") + (F.col("stddev_close") * 2)
).withColumn(
    "lower_band", F.col("moving_avg_close") - (F.col("stddev_close") * 2)
).select(
    F.col("window.start").alias("start"),
    F.col("window.end").alias("end"),
    "pairid",
    "avg_num_trades",
    "avg_volume",
    "moving_avg_close",
    "upper_band",
    "lower_band"
)

# Volume stats

# volume_stats_df = df.groupBy(window_spec).agg(
#     F.sum("taker_quote_asset_volume").alias("total_taker_quote_asset_volume"),
#     F.avg("taker_quote_asset_volume").alias("avg_taker_quote_asset_volume")
# )

# joined_df = df.join(
#     volume_stats_df, 
#     [F.col("open_time").between(volume_stats_df["window.start"], volume_stats_df["window.end"])],
#     "left"
# )

# joined_df = joined_df.withColumn(
#     "is_above_avg",
#     F.when(F.col("taker_quote_asset_volume") > F.col("avg_taker_quote_asset_volume"), 1).otherwise(0)
# )

# final_df = joined_df.groupBy(window_spec).agg(
#     F.sum("total_taker_quote_asset_volume").alias("sum_total_taker_quote_asset_volume"),
#     F.avg("avg_taker_quote_asset_volume").alias("avg_avg_taker_quote_asset_volume"),
#     F.sum("is_above_avg").alias("count_above_avg")
# ).select(
#     F.col("window.start").alias("window_start"),
#     F.col("window.end").alias("window_end"),
#     "sum_total_taker_quote_asset_volume",
#     "avg_avg_taker_quote_asset_volume",
#     "count_above_avg"
# )

final_df = df.groupBy(window_spec).agg(
    F.sum("quote_asset_volume").alias("total_quote_asset_volume"),
    F.avg("quote_asset_volume").alias("avg_quote_asset_volume")
).select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    "total_quote_asset_volume",
    "avg_quote_asset_volume"
)

df = df.withColumn("minute_start_time", (F.unix_timestamp(F.col("open_time")) - F.unix_timestamp(F.col("open_time")) % 60).cast("timestamp"))
minute_price_df = df.groupBy("pairid", "minute_start_time").agg(F.first(F.col("open")).alias("minute_start_price")).alias("minute_price_df")

minute_price_df = minute_price_df.withWatermark("minute_start_time", "10 minutes")

df_alias = df.alias("df_alias")

df_with_start_price = df_alias.join(minute_price_df, (F.col("df_alias.pairid") == F.col("minute_price_df.pairid")) &
                                    (F.col("df_alias.open_time") >= F.col("minute_price_df.minute_start_time")), "left")

threshold = 1  
df_with_start_price = df_with_start_price.withColumn("price_change_percent", 
                                                     ((F.col("close") - F.col("minute_start_price")) / F.col("minute_start_price")) * 100)

df_with_start_price = df_with_start_price.withColumn("price_alert", 
                                                     F.when(F.abs(F.col("price_change_percent")) >= threshold, "ALERT").otherwise("NORMAL"))

alert_df = df_with_start_price.filter(F.col("price_alert") == "ALERT")

def write_alerts_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("price_movement_alerts", mode="append")

def write_bollinger_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("real_time_statistics", mode="append")

def write_total_volume_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("real_time_volume_stats", mode="append")

query1 = bollinger_df.writeStream.outputMode("update").trigger(processingTime='5 minutes').foreachBatch(write_bollinger_to_hive).start()
query2 = final_df.writeStream.outputMode("update").trigger(processingTime='5 minutes').foreachBatch(write_total_volume_to_hive).start()
#query3 = alert_df.writeStream.outputMode("append").trigger(processingTime='5 minutes').foreachBatch(write_alerts_to_hive).start()

query1.awaitTermination()
query2.awaitTermination()
#query3.awaitTermination()
