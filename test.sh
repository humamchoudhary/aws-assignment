#!/bin/bash

for i in {1..20}
do
  # current timestamp in ms
  ts=$(date +%s%3N)
  
  # random value between 60 and 70 (with one decimal)
  value=$(awk -v min=60 -v max=70 'BEGIN{srand(); printf "%.1f", min+rand()*(max-min)}')
  
  curl --location 'https://991y9p57v1.execute-api.ap-southeast-2.amazonaws.com/dev/events' \
       --header 'Content-Type: application/json' \
       --data "{
         \"device_id\": \"D1\",
         \"type\": \"temperature\",
         \"value\": $value,
         \"ts\": $ts
       }"
  
  # optional: sleep a tiny bit between requests
  # sleep 0.1
done

