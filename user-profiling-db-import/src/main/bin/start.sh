#!/usr/bin/env bash

VERSION=`head -1 profiling-version`

# start full
JAR_PATH=/home/work/profiling-deploy
JAVA_HOME=/home/work/jdk1.7.0_79
LOG_PATH=/home/work/profiling-logs

nohup ${JAVA_HOME}/bin/java -cp ${JAR_PATH}/user-profiling-db-import-${VERSION}.jar \
db.load.FullTableLoad '2016-07-15 12:00:00' '2016-08-05 12:00:00' > ${LOG_PATH}/db_follow_full_load_`date '+%Y%m%d'` 2>&1 &

# start stream
JAVA_HOME=/home/work/jdk1.7.0_79
JAR_PATH=/home/work/profiling-deploy
LOG_PATH=/home/work/profiling-logs

nohup ${JAVA_HOME}/bin/java -cp ${JAR_PATH}/user-profiling-db-import-${VERSION}.jar \
db.load.FollowTimerLoad > ${LOG_PATH}/db_follow_stream_load_`date '+%Y%m%d'` 2>&1 &
