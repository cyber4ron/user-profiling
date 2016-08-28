#!/usr/bin/env bash

# 注意一个bulk的size, 5-15MB. 一般几秒返回.

# doc用lucene friendly id, 避免过于随机, 提高压缩率, 如用uuid-1

# 设置硬盘io速度, 但避免对search的影响. 根据ssd or机械
# es会防止index速度超过segments merge速度太多, 防止segment过多, 以及merge占用资源过多影响search性能.
# 2.3中, 用feedback loop to auto-throttle

# 关掉副本, 不强制refresh. 会快将近一倍.
curl -XPUT '10.10.35.14:9200/customer/_settings' -d '{ "index" : { "number_of_replicas" : 0 }}'
curl -XPUT '10.10.35.14:9200/customer/_settings' -d '{ "index" : { "refresh_interval" : -1 }}'

# setting queue size of bulk thread pool

# 机械硬盘的话, 可以设置index.merge.scheduler.max_thread_count: 1, 避免并发io

# bulk后还原

GET _cat/indices/customer_touring_20160523

##


PUT /customer_20160602
PUT /customer_20160602/_mapping/customer
...

PUT /house_20160602
PUT /house_20160602/_mapping/house
...

PUT /customer_delegation_20160602
PUT /customer_delegation_20160602/_mapping/delegation
...


PUT /customer_touring_20160602
PUT /customer_touring_20160602/_mapping/touring
...


PUT /customer_contract_20160602
PUT /customer_contract_20160602/_mapping/contract
...


PUT /customer_20160602/_settings
{ "index" : { "number_of_replicas" : 0 }}
PUT /customer_20160602/_settings
{ "index" : { "refresh_interval" : -1 }}

PUT /house_20160602/_settings
{ "index" : { "number_of_replicas" : 0 }}
PUT /house_20160602/_settings
{ "index" : { "refresh_interval" : -1 }}

PUT /customer_delegation_20160602/_settings
{ "index" : { "number_of_replicas" : 0 }}
PUT /customer_delegation_20160602/_settings
{ "index" : { "refresh_interval" : -1 }}


PUT /customer_touring_20160602/_settings
{ "index" : { "number_of_replicas" : 0 }}
PUT /customer_touring_20160602/_settings
{ "index" : { "refresh_interval" : -1 }}


PUT /customer_contract_20160602/_settings
{ "index" : { "number_of_replicas" : 0 }}
PUT /customer_contract_20160602/_settings
{ "index" : { "refresh_interval" : -1 }}

GET /customer_20160602/_search

