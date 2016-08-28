#!/usr/bin/env bash

export JAVA_HOME=/home/work/jdk1.7.0_79

if [ $# -ne 1 ]; then
    echo "es_start.sh HOST"
else
    HOST=$1
    echo "host: "${HOST}
fi

GC="-Xloggc:logs/gc.log -XX:+PrintFlagsFinal -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=100M"
JMX="-Djava.rmi.server.hostname=$HOST -Dcom.sun.management.jmxremote.port=2999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

# heap < 32G
nohup /home/work/jdk1.7.0_79/bin/java -Xms32000m -Xmx32000m -Djava.awt.headless=true -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 \
-XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError -XX:+DisableExplicitGC ${GC} ${JMX} \
-Dfile.encoding=UTF-8 -Djna.nosys=true -Des.path.home=/home/work/elasticsearch-2.3.2 \
-cp /home/work/elasticsearch-2.3.2/lib/elasticsearch-2.3.2.jar:/home/work/elasticsearch-2.3.2/lib/* org.elasticsearch.bootstrap.Elasticsearch start >> es.log 2>&1 &


# e.g. ./es_start.sh 10.10.35.14
