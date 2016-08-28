
drop table if exists profiling.touring;
create table profiling.touring
row format delimited
fields terminated by '\t'
lines terminated by '\n' as
select del.phone_a,
       touring.id,
       touring.cust_pkid,
       touring.showing_uid,
       touring.showing_broker_code,
       touring.showing_begin_time,
       touring.total_feed_back,
       touring.biz_type,
       touring.app_id,
       touring.city_id
from data_center.dim_merge_showing_day as touring join data_center.dim_merge_custdel_day as del
on touring.cust_pkid=del.pkid
and touring.pt='${partition}'
and del.pt='${partition}';
