#!/bin/bash

echo_message() {
    echo "$(date +"%d-%m-%Y %T") - $1"
}

mkdir -p Log

echo_message "Running docker-compose up -d"
docker-compose up -d > Log/dc_logs.log 2>&1
if [ $? -ne 0 ]; then
    echo_message "Error executing docker-compose up -d. Output is in Log/dc_logs.log"
    exit 1
fi
echo_message "docker-compose executed successfully."

echo_message "Waiting for namenode to be ready..."
until [ "`docker inspect -f {{.State.Running}} namenode`"=="true" ]; do
    sleep 1;
    echo_message "Waiting for namenode..."
done
echo_message "Namenode is ready."

commands=(
    "winpty docker exec -it namenode bash -c 'hdfs dfs -rm -r -f /data'"
    "winpty docker exec -it namenode bash -c 'hdfs dfs -mkdir -p /data'"
    "winpty docker exec -it namenode bash -c 'hdfs dfs -put ./data/raw/*.parquet /data'"
    "winpty docker exec -it spark-master bash -c 'chmod +x ./batch/process.sh && ./batch/process.sh'"
)

for cmd in "${commands[@]}"; do
    echo_message "Executing: $cmd"
    eval $cmd
    if [ $? -ne 0 ]; then
        echo_message "Error occurred in executing: $cmd"
        exit 1
    fi
done

echo_message "All commands executed successfully"


