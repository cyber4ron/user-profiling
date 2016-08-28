
create table profiling.agent_trace(id String, code string, lon float, lat float, client_time string, server_time string, db_time string)
row format delimited
fields terminated by '\t';

load data inpath '/user/bigdata/profiling/agent_trace_example' overwrite into table `profiling.agent_trace`;

drop table profiling.agent_trace_ext;

create table profiling.agent_trace_ext as
select code, lon, lat, client_time, team_code, team_name, city_id, user_name from profiling.agent_trace trace
join ods.ods_agent_extend_info info
on trace.code = info.uc_id
and info.pt='20160727000000';


-- ES
select team_name
from agent_trace
where team_name is not null and team_name is not 'null'
and city_id = '110000'
group by team_name limit 10000

select * from agent_trace
where team_name = '华彩国际店A店' limit 10000


create table profiling.popular_house_tmp(house_id string)
row format delimited
fields terminated by '\t';

load data inpath '/user/bigdata/profiling/popular_house' overwrite into table `profiling.popular_house_tmp`;

select follow.house_id, house.city_id from profiling.popular_house_tmp follow
join data_center.dim_merge_house_day house
on follow.house_id = house.house_pkid
and house.pt='20160801000000'

DROP table profiling.agent_house;

create table profiling.agent_house as
select agent.ucid, hdic.hdic_house_id
from (select distinct(ucid) from profiling.agent_trace) agent
join data_center.house_showing_house house
on agent.ucid = house.agent_ucid
and house.see_time between '2016-07-19 00:00:00' and '2016-07-27 00:00:00'
and house.pt = '20160801000000'
join data_center.house_sell_new hdic
on house.house_code = hdic.house_code
and hdic.pt = '20160801000000'
group by agent.ucid, hdic.hdic_house_id;

ALTER TABLE profiling.agent_trace CHANGE code ucid string;

