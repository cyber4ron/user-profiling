#!/usr/bin/env bash

GET /online_user/_mapping/
GET /online_user/_search
GET /online_user/user/2000000005377949
GET /online_user/user/_search
GET /online_user/user/_search
{
  "query": {
    "has_child": {
      "type": "mobile_duration",
      "query": {
        "term": {
          "ts": 1460293160
        }
      }
    }
  }
}

GET /online_user/mobile_duration/_search
GET /online_user/mobile_other_click/_search
GET /online_user/mobile_other_click/_search
{
  "query": {
    "term": {
      "event_name": "evaluation_my_house"
    }
  }
}

GET /online_user/user/_search
{
  "query": {
    "has_child": {
      "type": "mobile_other_click",
      "query": {
        "term": {
          "os": "1"
        }
      }
    }
  }
}

GET /online_user/mobile_other_click/_search
{
  "query" : {
    "bool" : {
      "must" : [
        {
          "has_parent" : {
            "type" : "user",
            "query" : {
              "term" : {
                "user_id" : "2000000004562248"
              }
            }
          }
        }, {
          "term" : {
            "os": "1"
          }
        }
      ]
    }
  }
}

GET /online_user/user/_search
{
  "size" : 0,
  "aggs" : {
    "country" : {
      "terms" : {
        "field" : "user_id"
      },
      "aggs" : {
        "yy" : {
          "children" : {
            "type" : "mobile_other_click"
          },
          "aggs" : {
            "xx" : {
              "terms" : {
                "field" : "mobile_other_click.channel_id"
              }
            }
          }
        }
      }
    }
  }
}
GET /online_user/_mapping/

GET /online_user/user/_search
{
  "size" : 0,
  "aggs" : {
    "xx" : {
      "children" : {
        "type" : "mobile_other_click"
      },
      "aggs" : {
        "yy" : {
          "terms" : {
            "field" : "event_name"
          }
        }
      }
    }
  }
}

GET /online_user/mobile_other_click/_search

GET /online_user/user/_search
{
  "size" : 0,
  "query" : {
    "has_child" : {
      "type" : "mobile_other_click",
      "query" : {
        "bool" : {
          "must" : [
            {
              "term" : { "event_name" : "evaluation_my_house"}
            },
            {
              "range" : {
                "ts": {
                  "gt": 1450822419,
                  "lt": 1461577770
                }
              }
            }
          ]
        }
      }
    }
  },
  "aggs" : {
    "house_eval" : {
      "children" : {
        "type" : "mobile_other_click"
      },
      "aggs" : {
        "city" : {
          "terms" : {
            "field" : "city_id"
          },
          "aggs" : {
            "channel" : {
              "terms" : {
                "field" : "channel_id"
              }
            }
          }
        }
      }
    }
  }
}

GET house/_mapping/house

GET house/house/_search
{
  "size": 0,
  "aggs": {
    "xx": {
      "terms" : {
        "field" : "resblock_id"
      },
               "aggs": {
                 "avg_list_price":{
                   "cardinality": {
                     "field": "resblock_id"
                   }
                 }
              }
    }
  }
}

GET /online_user/_mapping/mobile_other_click

# 修改index/type
PUT /online_user/_mapping/mobile_other_click
{
  "_source": {
    "enabled": true
  },
  "properties": {
    "timestamp": {
      "type": "date",
      "format": "epoch_second"
    },
    "os": {
      "type": "string"
    },
    "location": {
      "type": "geo_point"
    },
    "city_id": {
      "type": "string"
    },
    "channel_id": {
      "type": "string"
    },
    "current_page": {
      "type": "string"
    },
    "event_name": {
      "type": "string"
    }
  },
  "_parent": {
    "type": "user"
  }
}


