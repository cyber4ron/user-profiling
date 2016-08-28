#!/usr/bin/env bash

select * from (select city_id, max(city_name), count(*) as cnt from data_center.dim_merge_house_day house
where pt = '20160808000000' group by city_id) A where A.cnt > 10;

select * from (select district_code, max(district_name), count(*) as cnt from data_center.dim_merge_house_day house
where pt = '20160808000000' group by district_code) A where A.cnt > 10;

select * from (select bizcircle_code, max(bizcircle_name), count(*) as cnt from data_center.dim_merge_house_day
where pt = '20160808000000' group by bizcircle_code) A  where A.cnt > 10;

select * from (select face, count(*) as cnt from data_center.dim_merge_house_day
where pt = '20160808000000' group by face) A  where A.cnt > 10;

select * from (select house_usage_code, max(house_usage_name), count(*) as cnt from data_center.dim_merge_house_day
where pt = '20160808000000' group by house_usage_code) A  where A.cnt > 10;

select A.* from profiling.uuid_dict A
join (select ucid, count(*) as cnt from profiling.uuid_dict
where pt = '20160808000000' group by ucid having cnt >=2 and cnt <= 10) B
on A.ucid = B.ucid and A.pt = '20160808000000'
