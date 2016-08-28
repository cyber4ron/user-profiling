
set mapred.reduce.tasks = 29;

-- todo: 挺慢的，为什么只有一个reducer？
-- 删30天谴的?
drop table if exists profiling.delegation;
create table profiling.delegation
partition by (pt string)
row format delimited
fields terminated by '\t'
lines terminated by '\n' as
select del.phone_a,
       del.cust_pkid,
       del.creator_code,
       del.created_time,
       del.invalid_time,
       del.holder_time,
       del.created_uid,
       del.created_code,
       del.invalid_uid,
       del.holder_uid,
       del.holder_code,
       need.biz_type,
       need.district_name,
       need.bizcircle_name,
       need.room_min,
       need.room_max,
       need.area_min,
       need.area_max,
       need.balcony
from data_center.dim_merge_custdel_day as del join data_center.dim_merge_cust_need_day as need
on del.cust_pkid = need.cust_pkid
   and del.pt='${partition}'
   and need.pt='${partition}';
