#!/usr/bin/env bash

hadoop fs -dus /hbase/data/tmp

hbase shell

list

scan 'tmp:tbl1'

truncate 'tmp:tbl1'

scan 'tmp:tbl1',  {FILTER => "PrefixFilter('f88b06d8-42ad-469d-a31c-bb')"}s

scan 'tmp:tbl1',{ VERSIONS => 10}

create_namespace 'profiling'

create "profiling:test", {NAME => 'web'}

# meta
scan 'hbase:meta',{FILTER=>"PrefixFilter('profiling:online_user_201605')"}

####

# new java.sql.Timestamp(1462362771000L)

# 本地debug Batch, -XX:MaxPermSize=256m

# -- hive临时表
# create table profiling.contract20160501 as select ...

# backtrace
# 关掉副本
curl -XPUT '10.10.35.14:9200/online_user_20160502/_settings' -d '{ "index" : { "number_of_replicas" : 0 }}'
# 不强制refresh
curl -XPUT '10.10.35.14:9200/online_user_20160502/_settings' -d '{ "index" : { "refresh_interval" : -1 }}'

PUT /online_user_20160502/_settings
{ "index" : { "number_of_replicas" : 0 }}

PUT /online_user_20160502/_settings
{ "index" : { "refresh_interval" : -1 }}


# http://jx-bd-hadoop00.lianjia.com:8088/cluster

# start shell
export JAVA_HOME=/home/work/jdk1.7.0_79 && export HADOOP_CONF_DIR=hadoop-conf && export HADOOP_USER_NAME=bigdata && bin/spark-shell \
--master yarn --deploy-mode client \
--driver-memory 48G --executor-memory 14G \
--executor-cores 1 --num-executors 32 \
--conf spark.ui.port=6060 \
--conf spark.yarn.am.memory=4G \
--conf spark.executor.extraJavaOptions="-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC" \
--conf spark.es.nodes=10.10.35.14:9200,10.10.35.15:9200,10.10.35.16:9200 \
--conf spark.es.mapping.date.rich=false \
--conf spark.network.timeout=300s \
--conf spark.core.connection.ack.wait.timeout=300s \
--conf spark.files.fetchTimeout=300s \
--conf spark.shuffle.io.connectionTimeout=300s \
--conf spark.yarn.am.waitTime=300s \
--conf spark.akka.timeout=300s \
--conf spark.rpc.askTimeout=300s \
--conf spark.rpc.lookupTimeout=300s \
--conf spark.akka.remote.startup-timeout=300s \
--jars /home/work/fl/user-profiling-hbase-0.1.0.jar


# --executor-memory 1G / spark.memory默认的话, yarn container会running beyond virtual memory limits:
# 16/05/07 16:05:32 ERROR cluster.YarnScheduler: Lost executor 1 on 10.10.16.14: Container marked as failed:
# container_1459253925558_260735_02_000002 on host: jx-bd-hadoop19.lianjia.com. Exit status: 143. Diagnostics:
# Container [pid=99721,containerID=container_1459253925558_260735_02_000002] is running beyond virtual memory limits.
# Current usage: 999.8 MB of 1.5 GB physical memory used; 3.2 GB of 3.1 GB virtual memory used. Killing container.
#
# update, 改为 --executor-memory 3G 还是会挂.
# ref: http://stackoverflow.com/a/21008262
#

GET _cat/indices
PUT /_cluster/settings
{
      "transient" : {
        "action.destructive_requires_name": false
      }
}
