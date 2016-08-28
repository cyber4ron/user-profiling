#!/usr/bin/env bash

# 00 02 * * * cd /home/work/profiling-bin && bash start_house_eval_backtrace.sh 30000 >> /var/log/pf_house_eval_backtrace 2>&1

if [ $# -eq 1 ]; then
    num=$1
fi

export JAVA=/home/work/jdk1.7.0_79/bin/java

HOST="$(/sbin/ifconfig | grep -A 1 'em1' | tail -1 | grep "inet addr:" | cut -d ':' -f 2 | cut -d ' ' -f 1)"
JMX_PORT=2997
JMX="-Djava.rmi.server.hostname=${HOST} -Dcom.sun.management.jmxremote.port=${JMX_PORT} -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

DEBUG="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=3998"

CLASSPATH=/home/work/profiling-deploy/user-profiling-house-eval-backtrace-0.2.0-SNAPSHOT.jar

nohup $JAVA -cp $CLASSPATH $DEBUG ${JMX} com.lianjia.profiling.backtrace.HouseEvalBacktraceFromKafka $num > /home/work/profiling-logs/backtrace_house_eval_`date +"%Y%m%d_%H%M%S"` 2>&1 &
