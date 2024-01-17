from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, IntegerType


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


schema = StructType([
    StructField("t", TimestampType()),
    StructField("o", FloatType()),
    StructField("h", FloatType()),
    StructField("c", FloatType()),
    StructField("l", FloatType()),
    StructField("v", FloatType()),
    StructField("n", IntegerType()),
    StructField("q", FloatType()),
    StructField("V", FloatType()),
    StructField("Q", FloatType()),
])

spark = SparkSession \
    .builder \
    .appName("BTCUSDT - Moving Averages") \
    .getOrCreate()

# quiet_logs(spark)

data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "btc-usdt") \
    .load()

df = data.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*")

result_df = df \
    .groupBy(F.window(F.col("t"), "10 seconds")) \
    .agg(F.avg("c").alias("moving_avg_close")) \
    .select("window.start", "window.end", "moving_avg_close")

query = result_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
