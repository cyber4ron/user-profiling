# -*- coding:utf-8 -*-

import datetime
import sys
from threading import Thread

from src.main.python import falcon_push

tables = ["ds_t_core_house"]

hql_tmpl = """hive -e "select count(*) from hdic.{0} where pt='{1}000000'" """

prefix = "hdic_tbl_"
hql_failed_prefix = prefix + "hql_failed_"
metric_dict = {}


# 改了下python commands.getstatusoutput
def getstatusoutput(cmd):
    """Return (status, stdout) of executing cmd in a shell."""
    print "running cmd: " + cmd

    import os
    pipe = os.popen('{ ' + cmd + '; }', 'r')
    text = pipe.read()
    sts = pipe.close()
    if sts is None: sts = 0
    if text[-1:] == '\n': text = text[:-1]

    print "cmd: %s returned, status: %d, output: %s" % (cmd, sts, text)
    return sts, text


def check(tbl_name, date, days_before):
    try:
        hql = hql_tmpl.format(tbl_name, date)
        (status, count_cur) = getstatusoutput(hql)
        if status != 0: raise Exception("hql: %s failed." % hql)

        date_ref = datetime.date(int(date[0:4]), int(date[4:6]), int(date[6:8])) - datetime.timedelta(days=days_before)
        hql = hql_tmpl.format(tbl_name, date_ref.strftime('%Y%m%d'))
        (status, count_ref) = getstatusoutput(hql)
        if status != 0: raise Exception("hql: %s failed." % hql)

        metric_dict[prefix + tbl_name + "_cnt"] = count_cur
        metric_dict[prefix + tbl_name + "_" + str(days_before) + "days_before_cnt"] = count_ref
        metric_dict[prefix + tbl_name + "_change_ratio"] = float(count_cur) / float(count_ref) if count_ref != 0 else 0.0

    except Exception, e:
        print e
        metric_dict[hql_failed_prefix + tbl_name] = 1


if __name__ == "__main__":
    date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    if len(sys.argv) > 1:
        date = sys.argv[1]  # e.g. 20160301

    days_before = 1
    if len(sys.argv) > 2:
        days_before = int(sys.argv[2])

    threads = []

    for tbl in tables:
        t = Thread(target=check, args=(tbl, date, days_before))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print metric_dict
    status = falcon_push.push(metric_dict)

    print "status: " + str(status)

    sys.exit(status)
