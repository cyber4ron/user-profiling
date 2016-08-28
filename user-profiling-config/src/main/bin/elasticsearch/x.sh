#!/usr/bin/env bash


PUT /customer_touring/_settings
{ "index" : { "max_result_window" : 500000 } }

GET /online_user_20160606/_forcemerge?only_expunge_deletes=true

# Deprecated in 2.1.0.
# Optimize API has been renamed to the force merge API.
POST /twitter/_optimize
