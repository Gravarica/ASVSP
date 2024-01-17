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

conf = SparkConf().setAppName("Statistics").setMaster("spark://spark-master:7077")
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
    df_temp = spark.read.option("mergeSchema", "true").schema(customSchema).parquet(HDFS_NAMENODE + file_path)
    df_temp = df_temp.withColumn("PairID", F.lit(pair))

    if consolidatedDf is None:
        consolidatedDf = df_temp
    else:
        consolidatedDf = consolidatedDf.union(df_temp)

# dfBTC = spark.read.option("mergeSchema", "true").schema(customSchema).parquet(
#     HDFS_NAMENODE + "/data/BTC-USDT.parquet")
# dfETH = spark.read.option("mergeSchema", "true").schema(customSchema).parquet(
#     HDFS_NAMENODE + "/data/ETH-USDT.parquet")
# dfBNB = spark.read.option("mergeSchema", "true").schema(customSchema).parquet(
#     HDFS_NAMENODE + "/data/BNB-USDT.parquet")
# dfADA = spark.read.option("mergeSchema", "true").schema(customSchema).parquet(
#     HDFS_NAMENODE + "/data/ADA-USDT.parquet")
# dfXRP = spark.read.option("mergeSchema", "true").schema(customSchema).parquet(
#     HDFS_NAMENODE + "/data/XRP-USDT.parquet")

# dfBTC = dfBTC.withColumn("PairID", F.lit("BTC-USDT"))
# dfETH = dfETH.withColumn("PairID", F.lit("ETH-USDT"))
# dfBNB = dfBNB.withColumn("PairID", F.lit("BNB-USDT"))
# dfADA = dfADA.withColumn("PairID", F.lit("ADA-USDT"))
# dfXRP = dfXRP.withColumn("PairID", F.lit("XRP-USDT"))

# consolidatedDf = dfBTC.union(dfETH).union(dfBNB).union(dfADA).union(dfXRP)
consolidatedDf = consolidatedDf.withColumn("date", F.to_date("open_time"))

# Za rsi
daily_df = consolidatedDf.groupBy("PairID", "date").agg(
    F.first("open").alias("DailyOpen"),
    F.last("close").alias("DailyClose"),
    F.max("high").alias("DailyHigh"),
    F.min("low").alias("DailyLow")
)

windowSpec = Window.partitionBy("PairID").orderBy("date")

# Za volatilnost
dfWithReturns = consolidatedDf.withColumn("PreviousClose", F.lag("close", 1).over(windowSpec)) \
                              .withColumn("DailyReturn", (F.col("close") - F.col("PreviousClose")) / F.col("PreviousClose"))
# Pokretni proseci
windowSpec20 = Window.partitionBy("PairID").orderBy("date").rowsBetween(-20, Window.currentRow)
ma_df = daily_df.withColumn("MovingAverage", F.avg("DailyClose").over(windowSpec20))
ma_df = ma_df.withColumn("StdDev_20", F.stddev("DailyClose").over(windowSpec20))
ma_df = ma_df.withColumn("UpperBand", F.col("MovingAverage") + (F.col("StdDev_20") * 2))
ma_df = ma_df.withColumn("LowerBand", F.col("MovingAverage") - (F.col("StdDev_20") * 2))

# Za RSI
rsi_df = daily_df.withColumn("PrevClose", F.lag("DailyClose").over(windowSpec))
rsi_df = rsi_df.withColumn("DailyReturn", (F.col("DailyClose") - F.col("PrevClose")) / F.col("PrevClose"))

# Racunam volatilnost 
volatilityDf = dfWithReturns.filter(F.col("DailyReturn").isNotNull()) \
                            .groupBy("PairID", "date") \
                            .agg(F.stddev("DailyReturn").alias("Volatility"))

# Calculate RSI
gain_loss_df = rsi_df.withColumn("Gain", F.when(F.col("DailyReturn") > 0, F.col("DailyReturn")).otherwise(0)) \
                           .withColumn("Loss", F.when(F.col("DailyReturn") < 0, -F.col("DailyReturn")).otherwise(0))

windowSpec14 = Window.partitionBy("PairID").orderBy("date").rowsBetween(-13, Window.currentRow)

avg_gain_loss_df = gain_loss_df.withColumn("AvgGain", F.avg("Gain").over(windowSpec14)) \
                               .withColumn("AvgLoss", F.avg("Loss").over(windowSpec14))

rsi_df = avg_gain_loss_df.withColumn("RS", F.col("AvgGain") / F.col("AvgLoss")) \
                         .withColumn("RSI", 100 - (100 / (1 + F.col("RS"))))

# Handle possible division by zero in RSI calculation
rsi_df = rsi_df.withColumn("RSI", F.when(F.col("AvgLoss") == 0, 100).otherwise(F.col("RSI")))

final_df = volatilityDf.join(rsi_df, ["PairID", "Date"]).join(ma_df, ["PairID", "date"])

# Final DataFrame
final_df = final_df.select("PairID", "date", "MovingAverage","UpperBand", "LowerBand", "Volatility", "RSI")
final_df.write.mode("overwrite").saveAsTable("technical_analysis")

print("Picked up data frame...")
print("Saved to table!")
