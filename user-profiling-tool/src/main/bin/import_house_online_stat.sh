#!/usr/bin/env bash

# 00 08 * * * cd /home/work/profiling-bin && bash import_house_online_stat.sh >> /var/log/pf_import_house_online_stat 2>&1

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
--class com.lianjia.profiling.tool.DailyImportHouseOnlineStat \
--master yarn --deploy-mode client \
--queue default \
--driver-memory 2G --executor-memory 3G \
--executor-cores 1  --num-executors 5 \
--conf spark.ui.port=8099 \
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
--conf spark.task.maxFailures=8 \
--conf spark.es.nodes=10.10.35.14:9200,10.10.35.15:9200,10.10.35.16:9200 \
--conf spark.es.mapping.date.rich=false \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
${JAR_DIR}/user-profiling-tool-${VERSION}.jar ${date} > ${LOG_DIR}/import_house_online_stat_${date}.log 2>&1 &

echo "rc: $?, `date '+%Y%m%d' -d "yesterday"`"
