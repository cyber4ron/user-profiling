#!/usr/bin/env bash

export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
cd /Users/Eden/work_lj/user-profiling && mvn -Pprod -Dmaven.test.skip=true clean package install deploy -pl user-profiling-log-client -am
