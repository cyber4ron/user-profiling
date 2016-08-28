#!/usr/bin/env bash

POST /_aliases
{
    "actions" : [
        { "remove" : { "index" : "customer_contract_20160525", "alias" : "customer_contract" } },
        { "add" : { "index" : "customer_contract_20160602", "alias" : "customer_contract" } }
    ]
}

POST /_aliases
{
    "actions" : [
        { "remove" : { "index" : "customer_touring_20160525", "alias" : "customer_touring" } },
        { "add" : { "index" : "customer_touring_20160602", "alias" : "customer_touring" } }
    ]
}

POST /_aliases
{
    "actions" : [
        { "remove" : { "index" : "customer_delegation_20160525", "alias" : "customer_delegation" } },
        { "add" : { "index" : "customer_delegation_20160602", "alias" : "customer_delegation" } }
    ]
}


POST /_aliases
{
    "actions" : [
        { "remove" : { "index" : "customer_20160525", "alias" : "customer" } },
        { "add" : { "index" : "customer_20160602", "alias" : "customer" } }
    ]
}

POST /_aliases
{
    "actions" : [
        { "remove" : { "index" : "house_20160522", "alias" : "house" } },
        { "add" : { "index" : "house_20160602", "alias" : "house" } }
    ]
}

DELETE customer_contract_20160525

DELETE customer_touring_20160525

DELETE customer_delegation_20160525

DELETE customer_20160525


GET /customer_20160602/_settings
GET /house_20160602/_settings
GET /customer_contract_20160602/_settings
GET /customer_delegation_20160602/_settings
GET /customer_touring_20160602/_settings


DELETE customer_20160602
DELETE house_20160602
DELETE customer_contract_20160602
DELETE customer_delegation_20160602
DELETE customer_touring_20160602

POST /_aliases
{
    "actions" : [

        { "add" : { "index" : "house_prop_20160612", "alias" : "house" } }
    ]
}

