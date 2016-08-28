#!/usr/bin/env bash

# 给customer_20160501添加alias
PUT customer_20160501/_alias/customer
# 或
curl -XPOST 'http://10.10.35.14:9200/_aliases' -d '
{
    "actions" : [
        { "add" : { "index" : "customer_20160501", "alias" : "customer" } }
    ]
}'

# reindex

# 切换
curl -XPOST 'http://10.10.35.14:9200/_aliases' -d '
{
    "actions" : [
        { "remove" : { "index" : "customer_20160502", "alias" : "customer" } },
        { "add" : { "index" : "customer_20160501", "alias" : "customer" } }
    ]
}'

# check
GET /*/_alias/customer

# auto建index似乎有问题, primary shard找不到之类错误

# 一个人成交多次:
# select count(*) from data_center.dim_merge_contract_day where pt='20160503000000'
# 782486
# select count(*) from (select 1 from data_center.dim_merge_contract_day where pt='20160503000000' group by cott_pkid) t;
# 760845

# 融合层成交表，
# select count(*) from data_center.dim_merge_contract_day where pt='20160501000000'; 这个是781165
# select count(*) from data_center.dim_merge_contract_day where pt='20160502000000'; 这个是746863
# 如果表是全量的话貌似不正常。


val inc = getInc(sc, hiveCtx.read.table(s"profiling.touring_house20160526").map(_.mkString("\t")),
hiveCtx.read.table(s"profiling.touring_house20160525").map(_.mkString("\t")), "20160526", "20160525", "touring_house")


val inc2 = getInc(sc, hiveCtx.read.table(s"profiling.delegation20160524_for_test").map(_.mkString("\t")),
hiveCtx.read.table(s"profiling.delegation20160523").map(_.mkString("\t")), "20160524", "20160523", "delegation")

hiveCtx.sql("select * from profiling.touring_house20160525 where phone_a in ('15002297020', '15101644679', '13212096619')")

create table profiling.phone_ucid(phone string, ucid string)
row format delimited
fields terminated by '\t';

LOAD DATA INPATH '/user/bigdata/profiling/ucid_phone_20160724' OVERWRITE INTO TABLE `profiling.phone_ucid`;
