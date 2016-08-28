
drop table profiling.uuid_dict;

create table profiling.uuid_dict(uuid string, ucid string, source string, ts bigint)
partitioned by (pt string)
row format delimited
fields terminated by '\t';

alter table profiling.uuid_dict drop partition(pt='20160801000000')

alter table profiling.uuid_dict add partition(pt='20160803000000') location '/user/bigdata/profiling/cxx2'


