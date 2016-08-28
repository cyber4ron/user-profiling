#!/usr/bin/env bash

# profiling, dm00/dm01/dm02
export JAVA_HOME=/home/work/jdk1.8.0_65
bin/start.sh --server.port=8080 > web.log

http://10.10.35.15:8080/updatePopularHousePercentile?showingPct=0.05&followPct=0.05&token=jeRPp5FAGN7vdSD9

# house eval
# hadoop 01
export JAVA_HOME=/home/work/jdk1.8.0_40
bin/start.sh --server.port=8085 > web.log

# server02
bin/start.sh --server.port=8085 > web.log
