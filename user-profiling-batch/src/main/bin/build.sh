#!/usr/bin/env bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
cd /Users/Eden/work_lj/user-profiling && mvn -Dmaven.test.skip=true clean package -pl user-profiling-batch -am && \
cd user-profiling-batch/target && scp user-profiling-batch-0.2.0-SNAPSHOT.jar work@10.10.35.14:~/fl

# dm00
export JAVA_HOME=/home/work/jdk1.7.0_79
cd /home/work/profiling-code/profiling && /home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package install -pl user-profiling-batch -am && \
cd user-profiling-batch/target && cp user-profiling-batch-0.2.0-SNAPSHOT.jar ~/fl

export JAVA_HOME=/home/work/jdk1.7.0_79
cd /home/work/profiling-code/profiling/user-profiling-batch && /home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package install && \
cd target && cp user-profiling-batch-0.2.0-SNAPSHOT.jar ~/fl
