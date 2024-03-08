import os
import requests

# Base endpoint for Cloudflare's API for zones
BASE_URL = "https://api.cloudflare.com/client/v4/zones"

# Define Auth headers
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

# Set headers
headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}

phase = input("Enter phase to delete: ")

def get_user_zone_ids():
    # Prompt the user to enter zone IDs, separated by commas
    user_input = input("Enter the zone IDs, separated by commas: ")
    # Split the input string into a list of zone IDs
    zone_ids = [zone_id.strip() for zone_id in user_input.split(',')]
    return zone_ids

def delete_rules_current_ruleset(BASE_URL, headers, phase):
    zone_ids = get_user_zone_ids()
    
    # Iterate over the data from the first API
    for zone_id in zone_ids:

        # Call the rulesets API
        rulesets_api = BASE_URL + f"/{zone_id}/rulesets"
        response = requests.get(rulesets_api, headers=headers)
        data = response.json()
        
        # Iterate over the data from the rulesets API and filter for custom rules ruleset
        for rulesets_ids in data["result"]:
            if rulesets_ids["phase"] == phase:
                ruleset_id = rulesets_ids["id"]
                
                empty_payload = {}
                empty_payload["rules"] = []
                
                # Put empty payload into ruleset to nuke it
                rulesets_specific_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}"
                response = requests.put(rulesets_specific_api, headers=headers, json=empty_payload)
                data = response.json()
                
    return response
                
delete_rules_current_ruleset(BASE_URL, headers, phase)