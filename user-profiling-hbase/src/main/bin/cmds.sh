#!/usr/bin/env bash

export HADOOP_USER_NAME=bigdata
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
-Dhbase.hregion.max.filesize=10737418240 -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024 \
/user/bigdata/profiling/xxx profiling:event_online
