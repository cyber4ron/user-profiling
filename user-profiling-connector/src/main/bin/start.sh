#!/usr/bin/env bash

# distr, 做后台服务
export CLASSPATH=/home/work/profiling/user-profiling-connector-0.1.0.jar && \
export KAFKA_HEAP_OPTS="-Xmx2G" && \
nohup bin/connect-distributed.sh config/connect-distributed.properties > kafka-connect-house-eval.log 2>&1 &

# standalone
# bin/connect-standalone.sh config/connect-standalone.properties connector.properties

# add connector
# server00
curl -X POST -H "Content-Type: application/json" \
--data '{"name": "server00-house-eval-connector", "config": {"connector.class":"com.lianjia.profiling.connector.FileStreamSourceConnector", "tasks.max":"1", "topic":"bigdata-flow-02-log", "dir":"/home/work/House_Value/api_2.0/log/", "filename":"/home/work/House_Value/api_2.0/log/house_price.log"}}' \
http://172.16.5.24:9093/connectors/

# server01
curl -X POST -H "Content-Type: application/json" \
--data '{"name": "server01-house-eval-connector", "config": {"connector.class":"com.lianjia.profiling.connector.FileStreamSourceConnector", "tasks.max":"1", "topic":"bigdata-flow-02-log", "dir":"/home/work/House_Value/api_2.0/log/", "filename":"/home/work/House_Value/api_2.0/log/house_price.log"}}' \
http://172.16.5.25:9093/connectors/


######
# delete
curl -X "DELETE" http://172.16.5.24:8083/connectors/test
curl -X "DELETE" http://172.16.5.24:9093/connectors/server00-house-eval-connector
curl -X "DELETE" http://172.16.5.25:9093/connectors/server01-house-eval-connector


# 在http://docs.confluent.io/2.0.0/connect/userguide.html，搜那两个配置就行，
# config.storage.topic用来存connector和task配置（每个connector包含一组task）；
# offset.storage.topic存放connector的相关offset，类似__offsets。
# 另外config.storage.topic必须是一个partition，建议x3 replication；
# offset.storage.topic的话我这边数据量较小，可以3个partition，x3 replication.


####
# distributed方式有公共数据源可以用. 如果是tail本地log的话, standalone比较方便


##### standalone
# server00和server01在kafka/config目录下改好配置, export CLASSPATH, 然后start
export CLASSPATH=/home/work/profiling/user-profiling-connector-0.1.0.jar
export KAFKA_HEAP_OPTS="-Xmx1G"
nohup bin/connect-standalone.sh config/connect-standalone.properties config/connector.properties >> kafka-connect-house-eval.log 2>&1 &
