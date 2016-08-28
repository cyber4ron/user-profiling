#!/usr/bin/env bash

######### customer
delete /customer
PUT /customer
PUT /customer/_settings
{
    "index" : {
        "number_of_replicas" : 0
    }
}

PUT /customer/_mapping/customer


# delegation
delete /delegation
PUT /delegation
PUT /delegation/_settings
{
    "index" : {
        "number_of_replicas" : 0
    }
}

PUT /delegation/_mapping/delegation


# house
delete /house
PUT /house
PUT /house/_settings
{
    "index" : {
        "number_of_replicas" : 0
    }
}

PUT /house/_mapping/house


# touring
delete /touring
PUT /touring
PUT /touring/_settings
{
    "index" : {
        "number_of_replicas" : 0
    }
}

PUT /touring/_mapping/touring

PUT /touring/_mapping/touring_house
