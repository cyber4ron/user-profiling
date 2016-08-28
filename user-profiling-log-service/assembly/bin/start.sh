#!/usr/bin/env bash


# add libs to CLASSPATH
for f in lib/*.jar; do
CLASSPATH=${CLASSPATH}:$f;
done
for f in build/*.jar; do
CLASSPATH=${CLASSPATH}:$f;
done
CLASSPATH=${CLASSPATH}:lib/classes;
CLASSPATH=${CLASSPATH}:conf;

export CLASSPATH

java -Dlog4j.configuration=log4j-httpserver.properties -Xms2g -Xmx2g -verbose:gc -XX:+HeapDumpOnOutOfMemoryError -XX:+PrintGCDetails -XX:+PrintGCDateStamps com.lianjia.data.log.LogService $@
