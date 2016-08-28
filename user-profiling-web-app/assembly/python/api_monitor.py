# -*- coding:utf-8 -*-

import requests
import json
import sched, time

import falcon_push

SERVER1 = "10.10.9.16:8083"
SERVER2 = "172.16.5.25:9093"

url_stat = "http://{0}/{1}".format(SERVER1, "lianjia_hall_show/get_last_statistics?utime=1461553262")
url_detail = "http://{0}/{1}".format(SERVER1, "lianjia_hall_show/get_new_detail?start=0&end=1460946012")

url_stat2 = "http://{0}/{1}".format(SERVER2, "lianjia_hall_show/get_last_statistics?utime=1461553262")
url_detail2 = "http://{0}/{1}".format(SERVER2, "lianjia_hall_show/get_new_detail?start=0&end=1460946012")

FALCON_STAT_METRIC = "lj_screen_stat_api"
FALCON_DETAIL_METRIC = "lj_screen_detail_api"

scheduler = sched.scheduler(time.time, time.sleep)


def stat():
    try:
        r = requests.get(url_stat)
        status = r.status_code
        resp_json = json.loads(r.text)

        if status == 200 and "status" in resp_json and resp_json["status"] == 0:
            falcon_push.push({FALCON_STAT_METRIC: 0})
        else:
            falcon_push.push({FALCON_STAT_METRIC: 1})

        r = requests.get(url_stat2)
        status = r.status_code
        resp_json = json.loads(r.text)

        if status == 200 and "status" in resp_json and resp_json["status"] == 0:
            falcon_push.push({FALCON_STAT_METRIC: 0})
        else:
            falcon_push.push({FALCON_STAT_METRIC: 1})
    except Exception, e:
        print e
        falcon_push.push({FALCON_STAT_METRIC: 1})


def detail():
    try:
        r = requests.get(url_detail)
        status = r.status_code
        resp_json = json.loads(r.text)
        if status == 200 and "status" in resp_json and resp_json["status"] == 0:
            print falcon_push.push({FALCON_DETAIL_METRIC: 0})
        else:
            print falcon_push.push({FALCON_DETAIL_METRIC: 1})
        r = requests.get(url_detail2)
        status = r.status_code
        resp_json = json.loads(r.text)
        if status == 200 and "status" in resp_json and resp_json["status"] == 0:
            print falcon_push.push({FALCON_DETAIL_METRIC: 0})
        else:
            print falcon_push.push({FALCON_DETAIL_METRIC: 1})
    except Exception, e:
        print e
        print falcon_push.push({FALCON_DETAIL_METRIC: 1})


def cron_job():
    stat()
    detail()
    scheduler.enter(60 * 5, 1, cron_job, ())
    scheduler.run()

if __name__ == "__main__":
    cron_job()

"""
SECRET_KEY = 'mfTxlYCEcDgKFGtbLjBgEBWFCmKCoORE'
FALCON_PATH = 'http://minos-agent.lianjia.com/v1/push'
payload = {'secretkey': 'mfTxlYCEcDgKFGtbLjBgEBWFCmKCoORE', 'metrics': [{'tags': '', 'timestamp': 1461767495, 'metric': 'lj_screen_stat_api', 'value': 1, 'counterType': 'GAUGE', 'step': 300}], 'team': 'bigdata_profiling'}
r = requests.post(FALCON_PATH, data=json.dumps(payload))
print r
print r.text
"""
