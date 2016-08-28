#!/usr/bin/env bash

PUT /_snapshot/customer_backup_20160715
{
  "type": "fs",
  "settings": {
        "location": "/home/work/elasticsearch-snapshot",
        "compress": true
  }
}

