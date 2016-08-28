
drop table if exists profiling.touring_house;
create table profiling.touring_house
row format delimited
fields terminated by '\t'
lines terminated by '\n' as
select id,
       showing_id,
       created_uid,
       created_code,
       created_time,
       city_id,
       house_pkid,
       hdic_house_id,
       biz_type,
       frame_type,
       feedback_type,
       app_id       ,
       ctime,
       mtime
from data_center.dim_merge_showing_house_day
where pt='${partition}';
