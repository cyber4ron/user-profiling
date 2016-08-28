#!/usr/bin/env bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
cd /Users/Eden/work_lj/user-profiling && mvn -Pprod -Dmaven.test.skip=true clean package -pl user-profiling-bench -am && \
cd user-profiling-bench/target && scp user-profiling-bench-0.1.0.jar work@10.10.35.14:~/fl

