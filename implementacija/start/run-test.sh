winpty docker exec -it namenode bash -c "hdfs dfs -rm -r -f /test"
winpty docker exec -it namenode bash -c "hdfs dfs -mkdir -p /test"
winpty docker exec -it namenode bash -c "hdfs dfs -put ./test/raw/BTC-USDT.parquet /test"
winpty docker exec -it spark-master bash -c "chmod +x ./batch/process.sh && ./batch/process.sh"