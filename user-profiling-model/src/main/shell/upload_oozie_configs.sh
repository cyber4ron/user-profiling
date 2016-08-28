#!/usr/bin/env bash

# rm dirs
hadoop fs -rm -r /user/bigdata/oozie/profiling/customer/
hadoop fs -rm -r /user/bigdata/oozie/profiling/delegation/
hadoop fs -rm -r /user/bigdata/oozie/profiling/house/
hadoop fs -rm -r /user/bigdata/oozie/profiling/touring/
hadoop fs -rm -r /user/bigdata/oozie/profiling/touring_house/

# customer
hadoop fs -mkdir /user/bigdata/oozie/profiling/customer/
hadoop fs -mkdir /user/bigdata/oozie/profiling/customer/lib
hadoop fs -put customer/* /user/bigdata/oozie/profiling/customer/
hadoop fs -put user-profiling-batch-0.1.0.jar /user/bigdata/oozie/profiling/customer/lib

# delegation
hadoop fs -mkdir /user/bigdata/oozie/profiling/delegation/
hadoop fs -mkdir /user/bigdata/oozie/profiling/delegation/lib
hadoop fs -put delegation/* /user/bigdata/oozie/profiling/delegation/
hadoop fs -put user-profiling-batch-0.1.0.jar /user/bigdata/oozie/profiling/delegation/lib

# house
hadoop fs -mkdir /user/bigdata/oozie/profiling/house/
hadoop fs -mkdir /user/bigdata/oozie/profiling/house/lib
hadoop fs -put house/* /user/bigdata/oozie/profiling/house/
hadoop fs -put user-profiling-batch-0.1.0.jar /user/bigdata/oozie/profiling/house/lib

# touring
hadoop fs -mkdir /user/bigdata/oozie/profiling/touring/
hadoop fs -mkdir /user/bigdata/oozie/profiling/touring/lib
hadoop fs -put touring/* /user/bigdata/oozie/profiling/touring/
hadoop fs -put user-profiling-batch-0.1.0.jar /user/bigdata/oozie/profiling/touring/lib

# touring_house
hadoop fs -mkdir /user/bigdata/oozie/profiling/touring_house/
hadoop fs -mkdir /user/bigdata/oozie/profiling/touring_house/lib
hadoop fs -put touring_house/* /user/bigdata/oozie/profiling/touring_house/
hadoop fs -put user-profiling-batch-0.1.0.jar /user/bigdata/oozie/profiling/touring_house/lib

########## 临时脚本

hadoop fs -rm /user/bigdata/oozie/profiling/customer/*.xml
hadoop fs -put customer/*.xml /user/bigdata/oozie/profiling/customer/

hadoop fs -rm /user/bigdata/oozie/profiling/delegation/*.xml
hadoop fs -put delegation/*.xml /user/bigdata/oozie/profiling/delegation/

hadoop fs -rm /user/bigdata/oozie/profiling/house/*.xml
hadoop fs -put house/*.xml /user/bigdata/oozie/profiling/house/

hadoop fs -rm /user/bigdata/oozie/profiling/touring/*.xml
hadoop fs -put touring/*.xml /user/bigdata/oozie/profiling/touring/

hadoop fs -rm /user/bigdata/oozie/profiling/touring_house/*.xml
hadoop fs -put touring_house/*.xml /user/bigdata/oozie/profiling/touring_house/

####

hadoop fs -rm /user/bigdata/oozie/profiling/touring/*.sql
hadoop fs -put touring/*.sql /user/bigdata/oozie/profiling/touring/

####
oozie job -oozie http://jx-bd-off-hadoop00.lianjia.com:11000/oozie/ -config customer/job.properties -run
oozie job -oozie http://jx-bd-off-hadoop00.lianjia.com:11000/oozie/ -config delegation/job.properties -run
oozie job -oozie http://jx-bd-off-hadoop00.lianjia.com:11000/oozie/ -config house/job.properties -run
oozie job -oozie http://jx-bd-off-hadoop00.lianjia.com:11000/oozie/ -config touring/job.properties -run
oozie job -oozie http://jx-bd-off-hadoop00.lianjia.com:11000/oozie/ -config touring_house/job.properties -run
