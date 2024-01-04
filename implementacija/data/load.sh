echo "Deleting /result..."
hdfs dfs -rm -r -f /result
echo "Deleting /test..."
hdfs dfs -rm -r -f /test
echo "Creating /result..."
hdfs dfs -mkdir /result
echo "Creating /test..."
hdfs dfs -mkdir /test
echo "Putting /test..."
hdfs dfs -put ./test/BTC-USDT.parquet /test
echo "Finished..."