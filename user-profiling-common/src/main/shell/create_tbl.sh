#!/usr/bin/env bash

#drop_namespace 'tmp'
#create_namespace 'tmp'

drop_namespace 'profiling'
create_namespace 'profiling'
create 'profiling:customer', {NAME=>'ft', COMPRESSION => 'SNAPPY'}
create 'profiling:house', {NAME=>'ft', COMPRESSION => 'SNAPPY'}
create 'profiling:online_user', {NAME=>'web', COMPRESSION => 'SNAPPY'}, {NAME=>'mob', COMPRESSION => 'SNAPPY'}
create 'profiling:online_house', {NAME=>'web', COMPRESSION => 'SNAPPY'}, {NAME=>'mob', COMPRESSION => 'SNAPPY'}


create 'profiling:event_online2', { MAX_FILESIZE => '10737418240' }, {NAME=>'evt', BLOOMFILTER => 'ROW', VERSIONS => '2147483647'}

# list regions
scan 'hbase:meta',{FILTER=>"PrefixFilter('profiling:event_online')"}

# info:regioninfo - This qualifier contains STARTKEY and ENDKEY.
#
# info:server - This qualifier contains region server details

create 'profiling:online_user_prefer_201608v2', { NAME=>'prf', BLOOMFILTER => 'ROW', VERSIONS => '1'}, { MAX_FILESIZE => '10737418240' }

create 'profiling:online_user_event_201608', { NAME=>'evt', BLOOMFILTER => 'ROW', VERSIONS => '2147483647'}, { MAX_FILESIZE => '10737418240' }
