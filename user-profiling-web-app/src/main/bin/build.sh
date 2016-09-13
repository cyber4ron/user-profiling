#!/usr/bin/env bash

cd ~/work_lj/user-profiling/user-profiling-web-app && export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) \
&& mvn -Pprod -Dmaven.test.skip=true clean package \
&& cd target && scp user-profiling-web-app-1.0.tar.gz  work@jx-bd-server02.lianjia.com:/home/work/house_eval

# install依赖模块
cd ~/work_lj/user-profiling && mvn -Dmaven.test.skip=true clean package install -pl user-profiling-tagging -am
cd ~/work_lj/user-profiling && mvn -Dmaven.test.skip=true clean package install -pl user-profiling-util -am
cd ~/work_lj/user-profiling && mvn -Dmaven.test.skip=true clean package install -pl user-profiling-common -am

export JAVA_HOME=/home/work/jdk1.8.0_65
cd ~/work_lj/user-profiling/user-profiling-web-app && export JAVA_HOME=$(/usr/libexec/java_home -v 1.8) \
&& mvn -Pprod -Dmaven.test.skip=true clean package \
&& cd target && scp user-profiling-web-app-1.0.tar.gz  work@10.10.35.14:/home/work/fl

# dm00
export JAVA_HOME=/home/work/jdk1.7.0_79
/home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package install deploy -pl user-profiling-util -am

/home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package install -pl user-profiling-common -am

/home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package install -pl user-profiling-tagging -am

export JAVA_HOME=/home/work/jdk1.8.0_65
cd /home/work/profiling-code/profiling/user-profiling-web-app && /home/work/install/apache-maven-3.3.9/bin/mvn -Pprod -T 4 \
-Dmaven.test.skip=true clean package && \
cd target && cp user-profiling-web-app-1.0.tar.gz ~/fl



