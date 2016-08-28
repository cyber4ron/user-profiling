#!/usr/bin/env bash

if [ -z "${JAVA_HOME}" ]
then
    echo "missing JAVA_HOME.";
    exit 1
fi

JAVA=${JAVA_HOME}/bin/java

for f in lib/*.jar; do
CLASSPATH=${CLASSPATH}:$f;
done
for f in build/*.jar; do
CLASSPATH=${CLASSPATH}:$f;
done
CLASSPATH=${CLASSPATH}:lib/classes;
CLASSPATH=${CLASSPATH}:conf;

MAIN_CLASS="com.lianjia.profiling.web.bootstrap.Application"

HOST="$(/sbin/ifconfig | grep -A 1 'em1' | tail -1 | grep "inet addr:" | cut -d ':' -f 2 | cut -d ' ' -f 1)"
JMX_PORT=2998
JMX="-Djava.rmi.server.hostname=${HOST} -Dcom.sun.management.jmxremote.port=${JMX_PORT} -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

DEBUG="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=3999"

HEAP="-Xms4G -Xmx4G"
GC="-verbose:gc -XX:+HeapDumpOnOutOfMemoryError -Xloggc:gc-profiling-web.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=100M"

FLAGS="-server"

${JAVA} ${FLAGS} ${HEAP} ${GC} ${DEBUG} ${JMX} -cp ${CLASSPATH} ${MAIN_CLASS} $@ > web.log 2>&1 &
SERVER_PID=$!
echo ${SERVER_PID} > .pid_profiling_web
echo "profiling api server started, pid is ${SERVER_PID}."

#python python/api_monitor.py &
#echo "api monitor started."
