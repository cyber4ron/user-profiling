#!/usr/bin/env bash

export HADOOP_USER_NAME=bigdata
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
-Dhbase.hregion.max.filesize=10737418240 -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024 \
/user/bigdata/profiling/xxx profiling:event_online

# create hbase table

create 'profiling:online_user_prefer_v3', { NAME=>'prf', BLOOMFILTER => 'ROW', VERSIONS => '1'}, {MAX_FILESIZE => '10737418240'}

