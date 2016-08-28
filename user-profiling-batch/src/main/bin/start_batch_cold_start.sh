#!/usr/bin/env bash

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

# run specified:  --cold-start --run-specified --house --date ${date}
# --delegation --touring --touring-house --contract --house

export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \
export HADOOP_USER_NAME=bigdata && nohup ${SPARK_HOME}/bin/spark-submit \
--class com.lianjia.profiling.batch.Batch \
--master yarn --deploy-mode client \
--queue default \
--driver-memory 4G --executor-memory 3G \
--executor-cores 1 --num-executors 16 \
--conf spark.ui.port=8091 \
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
--conf spark.task.maxFailures=32 \
--conf spark.yarn.max.executor.failures=1600000 \
--conf spark.hive.table.waitMs=60000 \
--conf spark.es.cluster.name=profiling \
--conf spark.es.cluster.nodes=10.10.35.14:9300,10.10.35.15:9300,10.10.35.16:9300 \
--conf spark.logging.enable=true --conf spark.logging.host=172.16.5.21 --conf spark.logging.port=50051 \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
${JAR_DIR}/user-profiling-batch-${VERSION}.jar --cold-start --run-specified --house --date ${date} > ${LOG_DIR}/batch_cold_start_${date}_`date +"%H%M%S"`.log 2>&1 &

echo "${date}, rc of command: $?"
