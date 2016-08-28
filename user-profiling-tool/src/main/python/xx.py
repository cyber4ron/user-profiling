#-*- coding:utf-8 -*-

cnt = 0
with open("house_id2", 'w') as fout:
    with open("query_result (3).csv", 'r') as fin:
        for line in fin:
            cnt += 1
            if cnt == 1: continue
            parts = line.split(',')
            fout.write(parts[0][1:-1] + "\n")
