#!/usr/bin/env bash

export HADOOP_USER_NAME=bigdata && export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://jx-bd-hadoop00.lianjia.com:9000/user/bigdata/spark-events"
./sbin/start-history-server.sh
