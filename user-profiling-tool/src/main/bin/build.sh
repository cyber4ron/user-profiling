#!/usr/bin/env bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
cd /Users/Eden/work_lj/user-profiling && mvn -o -Pprod -Dmaven.test.skip=true clean package -pl user-profiling-tool -am && \
cd user-profiling-tool/target && scp user-profiling-tool-0.1.0.jar work@10.10.35.14:~/fl

# use nexus dependencies
export JAVA_HOME=/home/work/jdk1.7.0_79
cd /home/work/profiling-code/profiling/user-profiling-tool && /home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package install && \
cd target && cp user-profiling-tool-0.2.0-SNAPSHOT.jar ~/fl

