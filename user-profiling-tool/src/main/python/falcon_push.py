#-*- coding:utf-8 -*-

import requests
import time
import json

SECRECT_KEY = 'JsDHZURvcVrCdfbCIAJPGTsWaVdinNvk'
FALCON_PATH = 'http://minos-agent.lianjia.com/v1/push'

def push(metric_dict):
    ts = int(time.time())
    metric = {
        "metric" : "",
        "timestamp" : ts,
        "step" : 60,
        "value" : 0,
        "counterType" : "GAUGE",
        "tags":""
    }
    metrics = []
    for (k,v) in metric_dict.items():
        tmp = metric.copy()
        tmp['metric'] = k
        tmp['value'] = v
        metrics.append(tmp)
    payload = {
        "team":"datamining",
        "secretkey":SECRECT_KEY,
        "metrics":metrics
    }

    print payload

    r = requests.post(FALCON_PATH,data=json.dumps(payload))
    return json.loads(r.text)['status']


if __name__ == '__main__':
    r = push({
        'mail_test_zhangjy':0
    })
    print "status: " + str(r)
