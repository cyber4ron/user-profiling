
drop table if exists profiling.customer;
create table profiling.customer
row format delimited
fields terminated by '\t'
lines terminated by '\n' as
select phone_a,
       created_time,
       city_id,
       app_id
from data_center.dim_merge_custdel_day
where pt='${partition}';
