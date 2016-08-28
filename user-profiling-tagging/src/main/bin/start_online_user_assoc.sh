#!/usr/bin/env bash

if [ $# -eq 2 ]; then
    date=$1
    days=$2
else
    date=`date '+%Y%m%d' -d "yesterday"`
    days=1
fi

echo "date: $date"
echo "days: $days"

# 配置uuid_dict目录和house目录
# 回溯的话设置spark.backtrace.days
# backtrace:  bash start_online_user_assoc.sh 20160331 31

# 昨日daily:  bash start_online_user_assoc.sh

SPARK_HOME=/home/work/spark-1.6.1/
JAR_DIR=/home/work/profiling-deploy/
LOG_DIR=/home/work/profiling-logs/

VERSION=`head -1 profiling-version`

export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \
export HADOOP_USER_NAME=bigdata && nohup ${SPARK_HOME}/bin/spark-submit \
--class com.lianjia.profiling.tagging.AssocUcidAndUuid \
--master yarn --deploy-mode client \
--queue highPriority \
--driver-memory 8G --executor-memory 14G \
--executor-cores 1  --num-executors 14 \
--conf spark.yarn.am.memory=4G \
--conf spark.ui.port=8097 \
--conf user.name=bigdata \
--conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.network.timeout=2000s \
--conf spark.core.connection.ack.wait.timeout=2000s \
--conf spark.files.fetchTimeout=2000s \
--conf spark.shuffle.io.connectionTimeout=2000s \
--conf spark.yarn.am.waitTime=2000s \
--conf spark.akka.timeout=2000s \
--conf spark.rpc.askTimeout=2000s \
--conf spark.rpc.lookupTimeout=2000s \
--conf spark.akka.remote.startup-timeout=2000s \
--conf spark.shuffle.consolidateFiles=true \
--conf spark.task.maxFailures=4 \
--conf spark.es.cluster.name=profiling \
--conf spark.es.cluster.nodes=10.10.35.14:9300,10.10.35.15:9300,10.10.35.16:9300 \
--conf spark.enable.logging=true \
--conf spark.logging.host=172.16.5.21 \
--conf spark.logging.port=50051 \
--conf spark.backtrace.onlineUser.endDate=${date} \
--conf spark.backtrace.days=${days} \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
${JAR_DIR}/user-profiling-tagging-${VERSION}.jar > ${LOG_DIR}/backtrace_online_assoc_${date}_${days}.log 2>&1 &
