#!/usr/bin/env bash

SPARK_HOME=/home/work/spark-1.6.1/
JAR_DIR=/home/work/fl/
LOG_DIR=/home/work/profiling-logs/

export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \
export HADOOP_USER_NAME=bigdata && ${SPARK_HOME}/bin/spark-submit \
--class com.lianjia.profiling.tagging.HFileTest \
--master yarn --deploy-mode client \
--queue default \
--driver-memory 2G --executor-memory 2G \
--executor-cores 1  --num-executors 8 \
--conf spark.ui.port=6061 \
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
--conf spark.task.maxFailures=4 \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
${JAR_DIR}/user-profiling-tagging-0.1.0.jar
