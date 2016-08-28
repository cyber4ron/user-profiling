# -*- coding:utf-8 -*-

import sys
import commands
import time, datetime


def log(str):
    time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print "[{0}] {1}".format(time, str)


def check_hdfs_file_exist(path):
    log("checking path: {0}".format(path))

    check_cmd = "hadoop fs -test -e %s" % path
    (status, output) = commands.getstatusoutput(check_cmd)
    log("path: {0}, status: {1}, output: {2}".format(path, status, output))

    if status == 0:
        return True
    else:
        return False


def run_shell_command(shell_cmd):
    log("running {0} ...".format(shell_cmd))

    (status, output) = commands.getstatusoutput(shell_cmd)
    log("cmd: {0}, status: {1}, output: {2}".format(shell_cmd, status, output))

    return status, output


if __name__ == "__main__":

    date = sys.argv[1]

    BIN_DIR = "/home/work/profiling-bin"

    ONLINE_USER_EVENTS_PATH = "/user/bigdata/profiling/online_events_%date/_SUCCESS".replace("%date", date)
    ONLINE_USER_ASSOC_EVENTS_PATH = "/user/bigdata/profiling/online_user_assoc_%date/_SUCCESS".replace("%date", date)

    ###
    while not check_hdfs_file_exist(ONLINE_USER_EVENTS_PATH):
        log("{0} not exist, sleep and retry...".format(ONLINE_USER_EVENTS_PATH))
        time.sleep(60)

    cmd = "cd {0} && bash start_online_user_assoc.sh {1} {2}".format(BIN_DIR, date, 1)
    status, output = run_shell_command(cmd)

    if status != 0:
        sys.exit("run {0} failed, status: {1}, output: {2}".format(cmd, status, output))

    ###
    while not check_hdfs_file_exist(ONLINE_USER_ASSOC_EVENTS_PATH):
        log("{0} not exist, sleep and retry...".format(ONLINE_USER_ASSOC_EVENTS_PATH))
        time.sleep(60)

    cmd = "cd {0} && bash start_loading_to_hbase.sh {1}".format(BIN_DIR, date)
    status, output = run_shell_command(cmd)

    if status != 0:
        sys.exit("run {0} failed, status: {1}, output: {2}".format(cmd, status, output))
