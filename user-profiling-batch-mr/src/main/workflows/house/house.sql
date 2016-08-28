
-- todo 有code的就不用name了？关联的时候哪里可查，是否使分析变麻烦？
drop table if exists profiling.house;
create table profiling.house
row format delimited
fields terminated by '\t'
lines terminated by '\n' as
select house_pkid,
       hdic_house_id,
       created_uid,
       created_code,
       state,
       invalid_time,
       biz_type,
       total_prices,
       face,
       room_cnt,
       parlor_cnt,
       cookroom_cnt,
       toilet_cnt,
       balcony_cnt,
       build_area,
       district_code,
       district_name,
       bizcircle_code,
       bizcircle_name,
       resblock_id,
       resblock_name,
       building_id,
       building_name,
       unit_code,
       unit_name,
       signal_floor,
       house_usage_code,
       house_usage_name,
       total_floor,
       build_end_year,
       is_sales_tax,
       distance_metro_code,
       distance_metro_name,
       is_school_district,
       fitment_type_code,
       fitment_type_name   ,
       ctime,
       mtime
from data_center.dim_merge_house_day
where pt='${partition}';
