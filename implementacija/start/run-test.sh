docker exec -it namenode bash -c "chmod +x ./test/load.sh && ./test/load.sh"
docker exec -it spark-master bash -c "chmod +x ./stream/process.sh && ./stream/process.sh"