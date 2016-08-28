#!/usr/bin/env bash

# 00 04 * * * cd /home/work/profiling-bin && bash start_online_user_assoc_daily.sh >> /var/log/pf_online_user_assoc 2>&1

if [ $# -eq 1 ]; then
    date=$1
else
    date=`date '+%Y%m%d' -d "yesterday"`
fi

echo "date: $date"

SCRIPT_DIR='../profiling-script'
LOG_DIR='../profiling-logs'

export HADOOP_HOME="/home/work/hadoop"
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
export PATH=$PATH:$HADOOP_HOME/bin

hadoop version

nohup python ${SCRIPT_DIR}/assoc_user_events_and_load_to_hbase.py $date > ${LOG_DIR}/assoc_user_events_$date 2>&1 &

echo "date: ${date}, rc: $?"
