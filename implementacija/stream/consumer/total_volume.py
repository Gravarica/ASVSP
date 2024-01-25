from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, FloatType, LongType, IntegerType

conf = SparkConf().setAppName("taker_quote_volume_streaming").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

schema = StructType([
    StructField("open_time", LongType(), True),
    StructField("taker_quote_asset_volume", FloatType(), True),
])

topics = ["btc-usdt", "eth-usdt", "bnb-usdt"]

def process_volume_stream(topic):
    data = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:19092") \
            .option("subscribe", topic) \
            .option("kafka.group.id", "group2") \
            .load()

    df = data.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
    df = df.withColumn("open_time", (F.col("open_time") / 1000).cast("timestamp"))
    df = df.withColumn("pairid", F.lit(topic))
    return df

all_dfs = [process_volume_stream(topic) for topic in topics]
combined_df = all_dfs[0]
for df in all_dfs[1:]:
    combined_df = combined_df.union(df)

total_volume_df = combined_df.groupBy(
    F.window("open_time", "5 minutes")
).agg(
    F.sum("taker_quote_asset_volume").alias("total_taker_quote_asset_volume")
).select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    "total_taker_quote_asset_volume"
)

def write_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("total_volume_stats", mode="append")

query = total_volume_df.writeStream.outputMode("update").trigger(processingTime='5 minutes').foreachBatch(write_to_hive).start()

query.awaitTermination()
