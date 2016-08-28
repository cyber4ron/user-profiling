#!/usr/bin/env bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
cd /Users/Eden/work_lj/user-profiling && mvn -o -Pprod -Dmaven.test.skip=true clean package -pl user-profiling-tool -am && \
cd user-profiling-tool/target && scp user-profiling-tool-0.1.0.jar work@10.10.35.14:~/fl
