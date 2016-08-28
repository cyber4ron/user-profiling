#!/usr/bin/env bash

# 停止index, read only, optional

# disable shard allocation
PUT /_cluster/settings
{
    "transient" : {
        "cluster.routing.allocation.enable" : "none"
    }
}

# synced flush
POST /_flush/synced

# 重启node

PUT /_cluster/settings
{
  "transient": {
    "cluster.routing.allocation.enable": "all"


  }
}

GET _cat/nodes
GET _cat/health
GET _cat/recovery

# repeats

# resuming indexing


####

# ./es_start.sh 10.10.35.14
