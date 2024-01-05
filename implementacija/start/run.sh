winpty docker exec -it namenode bash -c "hdfs dfs -rm -r -f /data"
winpty docker exec -it namenode bash -c "hdfs dfs -mkdir -p /data"
winpty docker exec -it namenode bash -c "hdfs dfs -put ./data/raw/*.parquet /data"
#docker exec -it spark-master bash -c "chmod +x ./stream/process.sh && ./stream/process.sh"

