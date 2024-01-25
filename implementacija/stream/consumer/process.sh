# #!/bin/bash

# echo_message() {
#     echo "$(date +"%Y-%m-%d %T") - $1"
# }

# start_streaming_app() {
#     local app_script=$1
#     local log_file=$2

#     if [ ! -f "$app_script" ]; then
#         echo_message "Script not found: $app_script"
#         return 1
#     fi

#     local command="./../spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 $app_script > $log_file 2>&1 &"
#     echo_message "Starting: $command"
    
#     eval $command
#     local pid=$!
#     echo_message "Started $app_script (PID: $pid)"
# }

# script_dir="./../stream"
# log_dir="Log"

# mkdir -p $log_dir

# start_streaming_app "$script_dir/movingaverage.py" "$log_dir/movingaverage.log"
# start_streaming_app "$script_dir/total_volume.py" "$log_dir/total_volume.log"

# echo_message "All streaming applications have been started."

./../spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ./../stream/movingaverage.py
# ./../spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ./../stream/total_volume.py