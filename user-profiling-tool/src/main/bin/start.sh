#!/usr/bin/env bash

SPARK_HOME=/home/work/spark-1.6.1/
JAR_DIR=/home/work/profiling-deploy/
LOG_DIR=/home/work/profiling-logs/

# start shell
export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop-conf && \export HADOOP_USER_NAME=bigdata && \
${SPARK_HOME}/bin/spark-shell --master yarn --deploy-mode client --executor-cores 1 --num-executors 8 \
--conf spark.es.nodes=10.10.35.14:9200,10.10.35.15:9200,10.10.35.16:9200 \
--conf spark.es.mapping.date.rich=false \
--jars /home/work/fl/user-profiling-tool-0.1.0.jar


# start lj screen monitor
JAR_DIR=/home/work/profiling-deploy/lj-screen-monitor
LOG_DIR=/home/work/profiling-logs/
JAVA_HOME=/home/work/jdk1.7.0_79

nohup ${JAVA_HOME}/bin/java -cp ${JAR_DIR}/user-profiling-tool-0.1.0.jar \
com.lianjia.profiling.monitor.ljscreen.RoutineChecker > ${LOG_DIR}/lj-screen-stat-monitor.log 2>&1 &
