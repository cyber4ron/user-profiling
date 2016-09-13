#!/usr/bin/env bash

if [ $# -eq 1 ]; then
    date=$1
else
    date=`date '+%Y%m%d' -d "yesterday"`
fi

# assoc文件放在以日期结尾的路径上, 然后如
# bash start_loading_to_hbase.sh 20160331
# 或省略日期跑昨日

echo "date: $date"

SPARK_HOME=/home/work/spark-1.6.1/
JAR_DIR=/home/work/profiling-deploy/
LOG_DIR=/home/work/profiling-logs/

VERSION=`head -1 profiling-version`

export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \
export HADOOP_USER_NAME=bigdata && nohup ${SPARK_HOME}/bin/spark-submit \
--class com.lianjia.profiling.hbase.LoadHFile \
--master yarn --deploy-mode client \
--queue highPriority \
--driver-memory 8G --executor-memory 14G \
--executor-cores 1  --num-executors 14 \
--conf spark.yarn.am.memory=4G \
--conf spark.ui.port=8098 \
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
--conf spark.backtrace.date=${date} \
--conf spark.backtrace.userEvents=true \
--conf spark.backtrace.userPrefer=true \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
${JAR_DIR}/user-profiling-hbase-${VERSION}.jar > ${LOG_DIR}/load_to_hbase_${date}.log 2>&1 &
