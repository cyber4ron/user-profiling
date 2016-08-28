# -*- coding:utf-8 -*-

import urllib2
import sys

input = sys.argv[1]
output = sys.argv[2]
service = sys.argv[3]

cnt = 0
with open(output, 'w') as fout, open(input, 'r') as fin:
    for line in fin:
        parts = line.strip('\n').split('\t')
        phone = parts[0]

        url = 'http://{1}/kv/user/assoc/online/phone/{0}?token=jeRPp5FAGN7vdSD9'.format(phone, service)
        print url

        req = urllib2.Request(url)
        response = urllib2.urlopen(req)
        the_page = response.read()

        print(cnt)
        cnt += 1
        fout.write(line.strip('\n') + "\t" + the_page + "\n")

###

# nohup python get_online_event_by_phone.py customer_neg_without_show customer_neg_without_show_ph 10.10.35.14:8080 > customer_neg_without_show_log 2>&1 &
# nohup python get_online_event_by_phone.py customer_neg_with_show customer_neg_with_show_ph 10.10.35.14:8080 > customer_neg_with_show_log 2>&1 &
# nohup python get_online_event_by_phone.py customer_pos_with_show customer_pos_with_show_ph 10.10.35.14:8080 > customer_pos_with_show_log 2>&1 &
# nohup python get_online_event_by_phone.py customer_pos_without_show customer_pos_without_show_ph 10.10.35.14:8080 > customer_pos_without_show_log 2>&1 &
