#!/usr/bin/env bash

cd /Users/Eden/work_lj/user-profiling/user-profiling-batch/src/main/bin/
scp start_batch.sh start_daily_inc.sh work@10.10.35.14:~/profiling-bin
scp start_batch.sh start_daily_inc.sh work@10.10.35.15:~/profiling-bin

cd /Users/Eden/work_lj/user-profiling/user-profiling-stream/src/main/bin/
scp backtrace_house_eval.sh backtrace_online_user.sh start_streaming.sh start_streaming_daily_batch.sh start_ucid_mapping.sh work@10.10.35.14:~/profiling-bin
scp backtrace_house_eval.sh backtrace_online_user.sh start_streaming.sh start_streaming_daily_batch.sh start_ucid_mapping.sh work@10.10.35.15:~/profiling-bin
