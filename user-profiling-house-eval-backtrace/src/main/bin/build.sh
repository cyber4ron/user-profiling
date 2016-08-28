#!/usr/bin/env bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
cd /Users/Eden/work_lj/user-profiling && mvn -Dmaven.test.skip=true clean package -pl user-profiling-house-eval-backtrace -am && \
cd user-profiling-house-eval-backtrace/target && scp user-profiling-house-eval-backtrace-0.1.0.jar work@jx-bd-server00.lianjia.com:~/profiling

# dm00
export JAVA_HOME=/home/work/jdk1.7.0_79
cd /home/work/profiling-code/profiling && /home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package install -pl user-profiling-house-eval-backtrace -am && \
cd user-profiling-house-eval-backtrace/target && cp user-profiling-house-eval-backtrace-0.1.0.jar ~/fl

