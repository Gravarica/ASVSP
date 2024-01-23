#winpty docker exec -it namenode bash -c "hdfs dfs -rm -r -f /data"
#winpty docker exec -it namenode bash -c "hdfs dfs -mkdir -p /data"
#winpty docker exec -it namenode bash -c "hdfs dfs -put ./data/raw/*.parquet /data"
winpty docker exec -it spark-master bash -c "chmod +x ./batch/process.sh && ./batch/process.sh"

