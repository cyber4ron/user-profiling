#!/usr/bin/env bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
cd /Users/Eden/work_lj/user-profiling && mvn -Dmaven.test.skip=true clean package -pl user-profiling-connector -am && \
cd user-profiling-connector/target && scp user-profiling-connector-0.1.0.jar work@jx-bd-server00.lianjia.com:~/profiling

scp user-profiling-connector-0.1.0.jar work@jx-bd-server01.lianjia.com:~/profiling
