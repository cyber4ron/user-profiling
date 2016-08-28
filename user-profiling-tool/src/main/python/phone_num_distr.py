# -*- coding:utf-8 -*-

distr = {}
invalids = []
count = 0
DIGIT = 6

with open("phones", 'r') as fin:
    for line in fin:
        if len(line) < DIGIT or not line[0:DIGIT].isdigit():
            invalids.append(line)
        elif line[0:DIGIT] in distr:
            distr[line[0:DIGIT]] += 1
            count += 1
        else:
            distr[line[0:DIGIT]] = 1
            count += 1

distr_sorted = sorted(distr.items())
for kv in distr_sorted:
    if kv[1] > 100:
        print(kv[0] + '\t' + str(kv[1]))

print "invalids: " + str(len(invalids))
print "valid: " + str(count)

bucket = count / 16
print "bucket: " + str(bucket)
tmp = 0
for kv in distr_sorted:
    tmp += kv[1]
    if tmp > bucket:
        print kv[0] + "\t" + str(tmp)
        tmp = 0

# valid: 230981
# bucket: 14436
# 131642	14463
# 134260	14569
# 135220	14646
# 136180	14462
# 136922	14451
# 138012	14703
# 138202	14497
# 139106	14729
# 139819	14468
# 152118	14437
# 158028	14452
# 159986	14514
# 185159	14438
# 186119	14586
# 188011	14480
