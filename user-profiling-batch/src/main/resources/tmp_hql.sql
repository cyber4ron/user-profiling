
-- delegation
drop table profiling.delegation20160525;
create table profiling.delegation20160525 as
select
del.phone_a,
del.cust_pkid,
del.name,
del.state,
del.created_time,
del.invalid_time,
del.holder_time,
del.created_uid,
del.created_code,
del.invalid_uid,
del.holder_uid,
del.holder_code,
del.biz_type,
del.city_id,
del.app_id,
need.district_code,
need.district_name,
need.bizcircle_name,
need.room_min,
need.room_max,
need.area_min,
need.area_max,
need.price_min,
need.price_max,
need.balcony,
need.face
from data_center.dim_merge_custdel_day as del
join data_center.dim_merge_cust_need_day as need
on del.cust_pkid = need.cust_pkid
and del.app_id = need.app_id
and need.pt = '20160525000000'
where del.pt = '20160525000000';

-- touring
drop table profiling.touring20160525;
create table profiling.touring20160525 as
select
del.phone_a,
touring.id,
touring.cust_pkid,
touring.showing_uid,
touring.showing_broker_code,
touring.showing_begin_time,
touring.showing_end_time,
touring.total_feed_back,
touring.biz_type,
touring.app_id,
touring.city_id
from data_center.dim_merge_showing_day as touring
join data_center.dim_merge_custdel_day as del
on touring.cust_pkid = del.cust_pkid
and touring.app_id = del.app_id
and del.pt = '20160525000000'
where touring.pt = '20160525000000';

-- touring_house
drop table profiling.touring_house20160525;
create table profiling.touring_house20160525 as
select
del.phone_a,
touring.id as tid,
touring_house.id as thid,
touring_house.created_uid,
touring_house.created_code,
touring_house.created_time,
touring_house.city_id,
touring_house.house_pkid,
touring_house.hdic_house_id,
touring_house.biz_type,
touring_house.frame_type,
touring_house.feedback_type,
house.district_code,
house.district_name,
house.bizcircle_code,
house.bizcircle_name,
house.resblock_id,
house.resblock_name,
house.room_cnt,
round(house.build_area),
house.signal_floor,
house.floor_name,
house.total_floor,
house.build_end_year,
house.is_sales_tax,
house.distance_metro_code,
house.distance_metro_name,
house.is_school_district,
house.is_unique_house,
house.face,
round(house.total_prices)
from data_center.dim_merge_showing_house_day as touring_house
join data_center.dim_merge_showing_day as touring
on touring_house.showing_id = touring.id
and touring_house.app_id = touring.app_id
and touring.pt = '20160525000000'
join data_center.dim_merge_custdel_day as del
on touring.cust_pkid = del.cust_pkid
and touring.app_id = del.app_id
and del.pt = '20160525000000'
join data_center.dim_merge_house_day as house
on touring_house.house_pkid = house.house_pkid
and touring_house.app_id = house.app_id
and house.pt = '20160525000000'
where touring_house.pt = '20160525000000';

-- contract
drop table profiling.contract20160525;
create table profiling.contract20160525 as
select
del.phone_a,
contr.cott_pkid,
contr.cust_pkid,
contr.house_pkid,
contr.hdic_house_id,
contr.state,
contr.deal_time,
round(contr.realmoney),
contr.created_uid,
contr.created_code,
contr.created_time,
contr.biz_type,
contr.city_id,
contr.app_id,
house.district_code,
house.district_name,
house.bizcircle_code,
house.bizcircle_name,
house.resblock_id,
house.resblock_name,
house.room_cnt,
round(house.build_area),
house.signal_floor,
house.floor_name,
house.total_floor,
house.build_end_year,
house.is_sales_tax,
house.distance_metro_code,
house.distance_metro_name,
house.is_school_district,
house.is_unique_house,
house.face,
round(house.total_prices)
from data_center.dim_merge_contract_day as contr
join data_center.dim_merge_custdel_day as del
on contr.cust_pkid = del.cust_pkid
and contr.app_id = del.app_id
and contr.pt = '20160525000000'
and del.pt = '20160525000000'
join data_center.dim_merge_house_day as house
on contr.house_pkid = house.house_pkid
and contr.app_id = house.app_id
and house.pt = '20160525000000';

-- house
drop table profiling.house20160525;
create table profiling.house20160525 as
select
house_pkid,
hdic_house_id,
created_uid,
created_code,
state,
created_time,
invalid_time,
holder_time,
biz_type,
round(total_prices),
face,
room_cnt,
parlor_cnt,
cookroom_cnt,
toilet_cnt,
balcony_cnt,
round(build_area),
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
floor_name,
house_usage_code,
house_usage_name,
total_floor,
build_end_year,
is_sales_tax,
distance_metro_code,
distance_metro_name,
is_school_district,
fitment_type_code,
fitment_type_name,
city_id,
city_name,
is_unique_house,
app_id
from data_center.dim_merge_house_day as house
where house.pt = '20160525000000';
