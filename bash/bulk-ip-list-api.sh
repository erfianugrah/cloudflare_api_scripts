#!/bin/bash

X_AUTH_EMAIL=$CLOUDFLARE_EMAIL
X_AUTH_KEY=$CLOUDFLARE_API_KEY

# First API endpoint to get the IDs
first_api_url="https://api.cloudflare.com/client/v4/accounts"
account_ids=$(curl $first_api_url -H "X-Auth-Email: $X_AUTH_EMAIL" -H "X-Auth-Key: $X_AUTH_KEY" | jq -r '.result[] | .id')

for account_id in $account_ids; do
    # Second API endpoint to get data using the firewall rule IDs
    second_api_url="$first_api_url/$account_id/rules/lists?page=1&per_page=1000"
    lists=$(curl $second_api_url -H "X-Auth-Email: $X_AUTH_EMAIL" -H "X-Auth-Key: $X_AUTH_KEY" | jq -r '.result[] | select(.description == "managed_challenge IP list by crowdsec") | .id')
    
    for list in $lists; do
    # Third API endpoint to use data from the second API
    third_api_url="$first_api_url/$account_id/rules/lists/$list"
    curl -X "DELETE" $third_api_url -H "X-Auth-Email: $X_AUTH_EMAIL" -H "X-Auth-Key: $X_AUTH_KEY"

    done
done