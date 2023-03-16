#!/bin/bash

X_AUTH_EMAIL=$CLOUDFLARE_EMAIL
X_AUTH_KEY=$CLOUDFLARE_API_KEY

# First API endpoint to get the IDs
first_api_url="https://api.cloudflare.com/client/v4/zones"
zone_ids=$(curl $first_api_url -H "X-Auth-Email: $X_AUTH_EMAIL" -H "X-Auth-Key: $X_AUTH_KEY" | jq -r '.result[] | .id')

for zone_id in $zone_ids; do

    page=1

    # Second API endpoint to get data using the firewall rule IDs
    second_api_url="$first_api_url/$zone_id/firewall/rules?page=$page&per_page=10"
    while [ $(curl $second_api_url -H "X-Auth-Email: $X_AUTH_EMAIL" -H "X-Auth-Key: $X_AUTH_KEY" | jq '.result | length') -ne 0 ]
    do

    firewall_rule_ids=$(curl $second_api_url -H "X-Auth-Email: $X_AUTH_EMAIL" -H "X-Auth-Key: $X_AUTH_KEY" | jq -r '.result[] | select(.description == "CrowdSec managed_challenge rule") | .id')

    for firewall_rule_id in $firewall_rule_ids; do

    # Third API endpoint to use data from the second API
    third_api_url="$first_api_url/$zone_id/firewall/rules/$firewall_rule_id"
    curl -X "DELETE" $third_api_url -H "X-Auth-Email: $X_AUTH_EMAIL" -H "X-Auth-Key: $X_AUTH_KEY"
        done

        # Increment the page number for the next iteration
        (( page ++))
    done
done