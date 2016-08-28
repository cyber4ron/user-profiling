#!/usr/bin/env bash

# todo 有些参数已废弃

export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=hadoop-conf \
&& export HADOOP_USER_NAME=bigdata && nohup bin/spark-submit \
--class com.lianjia.profiling.batch.Batch \
--master yarn --deploy-mode client \
--queue default \
--driver-memory 2G --executor-memory 1G \
--executor-cores 1  --num-executors 32 \
--conf spark.ui.port=8089 \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
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
--conf spark.es.cluster.name=profiling \
--conf spark.es.cluster.nodes=10.10.35.14:9300,10.10.35.15:9300,10.10.35.16:9300 \
--conf spark.logging.enable=true \
--conf spark.logging.host=172.16.5.21 \
--conf spark.logging.port=50051 \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
/home/work/deploy/user-profiling-batch-0.1.0.jar --date 20160501 --parallel > batch.log 2>&1 &
