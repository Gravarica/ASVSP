#docker exec -it namenode bash -c "hdfs dfs -rm -r -f /data"
docker exec -it namenode bash -c "chmod +x ./data/load.sh && ./data/load.sh"
docker exec -it spark-master bash -c "chmod +x ./stream/process.sh && ./stream/process.sh"

