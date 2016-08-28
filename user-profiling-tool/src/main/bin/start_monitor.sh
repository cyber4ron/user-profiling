#!/usr/bin/env bash

VERSION=`head -1 profiling-version`

JAR_DIR=/home/work/profiling-deploy/es-monitor
nohup /home/work/jdk1.7.0_79/bin/java \
-cp ${JAR_DIR}/user-profiling-tool-${VERSION}.jar \
com.lianjia.profiling.tool.ProcessMonitor bootstrap.Elasticsearch es.node 60000 >> es_monitor.log 2>&1 &


JAR_DIR=/home/work/profiling-deploy/es-monitor
nohup /home/work/jdk1.7.0_79/bin/java \
-cp ${JAR_DIR}/user-profiling-tool-${VERSION}.jar \
com.lianjia.profiling.tool.ProcessMonitor node/b kibana 2000 >> kibana_monitor.log 2>&1 &
