#!/usr/bin/env bash

# todo 改
export HADOOP_CONF_DIR=/home/work/fl/spark-1.6.1/hadoop-conf && export HADOOP_USER_NAME=bigdata && bin/spark-submit \
--class com.lianjia.profiling.batch.spark.BatchScheduler \
--master yarn --deploy-mode client \
--driver-memory 4G --executor-memory 3500M \
--executor-cores 1  --num-executors 24 \
--conf spark.ui.port=8089 \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
--conf spark.network.timeout=120s \
--conf spark.core.connection.ack.wait.timeout=120s \
--conf spark.files.fetchTimeout=300s \
--conf spark.shuffle.io.connectionTimeout=300s \
--conf spark.yarn.am.waitTime=300s \
--conf spark.akka.timeout=300s \
--conf spark.rpc.askTimeout=300s \
--conf spark.rpc.lookupTimeout=300s \
--conf spark.akka.remote.startup-timeout=300s \
--conf spark.shuffle.consolidateFiles=true \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
/home/work/fl/user-profiling-batch-0.1.0.jar
