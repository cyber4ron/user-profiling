#!/usr/bin/env bash

# 00 03 * * * cd /home/work/profiling-bin && bash start_streaming_daily_dump_to_hdfs.sh >> /var/log/pf_start_streaming_daily_batch 2>&1

if [ $# -eq 0 ]; then
    date=`date '+%Y%m%d' -d "yesterday"`
elif [ $# -eq 1 ]; then
    date=$1
else
    echo "example: $0 20160501"
    exit 1
fi

SPARK_HOME=/home/work/spark-1.6.1/
JAR_DIR=/home/work/profiling-deploy/
LOG_DIR=/home/work/profiling-logs/

VERSION=`head -1 profiling-version`

export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \
export HADOOP_USER_NAME=bigdata && nohup ${SPARK_HOME}/bin/spark-submit \
--class com.lianjia.profiling.stream.backtrace.OnlineUserBacktrace \
--master yarn --deploy-mode client \
--queue default \
--driver-memory 4G --executor-memory 3G \
--executor-cores 1  --num-executors 64 \
--conf spark.ui.port=8095 \
--conf user.name=bigdata \
--conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.network.timeout=300s \
--conf spark.core.connection.ack.wait.timeout=300s \
--conf spark.files.fetchTimeout=300s \
--conf spark.shuffle.io.connectionTimeout=300s \
--conf spark.yarn.am.waitTime=300s \
--conf spark.akka.timeout=300s \
--conf spark.rpc.askTimeout=300s \
--conf spark.rpc.lookupTimeout=300s \
--conf spark.akka.remote.startup-timeout=300s \
--conf spark.shuffle.consolidateFiles=true \
--conf spark.task.maxFailures=8 \
--conf spark.es.cluster.name=profiling \
--conf spark.es.cluster.nodes=10.10.35.14:9300,10.10.35.15:9300,10.10.35.16:9300 \
--conf spark.enable.logging=true \
--conf spark.logging.host=172.16.5.21 \
--conf spark.logging.port=50051 \
--conf spark.backtrace.onlineUser.startDate=${date} \
--conf spark.backtrace.onlineUser.endDate=${date} \
--conf spark.backtrace.to.es=true \
--conf spark.backtrace.to.hbase=false \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
${JAR_DIR}/user-profiling-stream-${VERSION}.jar > ${LOG_DIR}/online_user_batch_daily_${date}.log 2>&1 &

echo "${date}, rc of command: $?"
