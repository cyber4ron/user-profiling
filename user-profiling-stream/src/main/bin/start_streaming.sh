#!/usr/bin/env bash

SPARK_HOME=/home/work/spark-1.6.1/
JAR_DIR=/home/work/profiling-deploy/
LOG_DIR=/home/work/profiling-logs/

VERSION=`head -1 profiling-version`

# kill
pid=`ps -eo '%p %a' | grep 'class com.lianjia.profiling.stream.Stream' | grep java | sed -e 's/^[[:space:]]*//' | cut -d' ' -f1`

kill ${pid}

echo "kill $pid returned, rc: $?"

# restart
export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \
export HADOOP_USER_NAME=bigdata && nohup ${SPARK_HOME}/bin/spark-submit \
--class com.lianjia.profiling.stream.Stream \
--master yarn --deploy-mode client \
--queue default \
--driver-memory 3G --executor-memory 2G \
--executor-cores 1  --num-executors 16 \
--conf spark.ui.port=8089 \
--conf spark.memory.fraction=0.75 \
--conf spark.memory.storageFraction=0.5 \
--conf spark.memory.offHeap.enabled=false \
--conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
--conf spark.shuffle.consolidateFiles=true \
--conf spark.network.timeout=300s \
--conf spark.core.connection.ack.wait.timeout=300s \
--conf spark.files.fetchTimeout=300s \
--conf spark.shuffle.io.connectionTimeout=300s \
--conf spark.yarn.am.waitTime=300s \
--conf spark.akka.timeout=300s \
--conf spark.rpc.askTimeout=300s \
--conf spark.rpc.lookupTimeout=300s \
--conf spark.akka.remote.startup-timeout=300s \
--conf spark.yarn.max.executor.failures=1600000 \
--conf spark.es.cluster.name=profiling \
--conf spark.es.cluster.nodes=10.10.35.14:9300,10.10.35.15:9300,10.10.35.16:9300 \
--conf spark.enable.logging=true \
--conf spark.logging.host=172.16.5.21 \
--conf spark.logging.port=50051 \
--conf spark.backtrace.to.es=true \
--conf spark.backtrace.to.hbase=false \
--conf spark.kafka.offset.from=latest \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
${JAR_DIR}/user-profiling-stream-${VERSION}.jar > ${LOG_DIR}/stream_`date +"%Y%m%d_%H%M%S"`.log 2>&1 &

echo "rc of command: $?"

