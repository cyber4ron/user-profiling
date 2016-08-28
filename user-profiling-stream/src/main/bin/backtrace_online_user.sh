#!/usr/bin/env bash

if [ $# -eq 2 ]; then
    date=$1
    weeks=$2
else
    echo "example: $0 20160512 25"
    exit 1
fi

# dump to hdfs
# --conf spark.backtrace.to.es=false \
# --conf spark.backtrace.to.hbase=false \

SPARK_HOME=/home/work/spark-1.6.1/
JAR_DIR=/home/work/profiling-deploy/
LOG_DIR=/home/work/profiling-logs/

VERSION=`head -1 profiling-version`

for ((i=0; i<${weeks}; i=i+1)); do

    date_from=$(date -d "$((i * 7 + 6)) day ago ${date}" +%Y%m%d)
    date_end=$(date -d "$((i * 7)) day ago ${date}" +%Y%m%d)

    export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \
    export HADOOP_USER_NAME=bigdata && ${SPARK_HOME}/bin/spark-submit \
    --class com.lianjia.profiling.stream.backtrace.OnlineUserBacktrace \
    --master yarn --deploy-mode client \
    --queue default \
    --driver-memory 6G --executor-memory 3G \
    --executor-cores 1  --num-executors 64 \
    --conf spark.ui.port=8094 \
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
    --conf spark.backtrace.onlineUser.startDate=${date_from} \
    --conf spark.backtrace.onlineUser.endDate=${date_end} \
    --conf spark.backtrace.to.es=false \
    --conf spark.backtrace.to.hbase=false \
    --conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
    ${JAR_DIR}/user-profiling-stream-${VERSION}.jar > ${LOG_DIR}/backtrace_online_user_${date_from}_${date_end}.log 2>&1

    echo "${date_from}-${date_end}, rc of command: $?"
done
