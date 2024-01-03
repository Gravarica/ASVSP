#docker exec -it namenode bash -c "hdfs dfs -rm -r -f /data"
docker exec -it namenode bash -c "chmod +x ./test/load.sh && ./test/load.sh"
docker exec -it spark-master bash -c "chmod +x ./batch/process.sh && ./batch/process.sh"

