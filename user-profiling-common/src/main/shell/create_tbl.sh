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
