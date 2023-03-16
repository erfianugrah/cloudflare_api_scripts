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

def loop_zone_id_pages(BASE_URL, headers):
    
# Set array of list of zone ids
    raw_zone_id_list = []
    page = 1
    while True:
        # Make a request to the zones API endpoint to get the zone ids
        response = requests.get(BASE_URL+ f"?page={page}&per_page=1000", headers=headers)
        
        # Catch error
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve data from the Zone IDs API. Status code: {response.status_code}")
        
        # Parse the response as JSON
        data = response.json()
        
        # Get the result payload
        raw_zone_ids = data["result"]
        
        # Add all zone_ids into list
        raw_zone_id_list.extend(raw_zone_ids)
        
        if not raw_zone_ids:
            break
        page += 1

    return raw_zone_id_list

def iterate_zone_ids_into_list(BASE_URL, headers):
    
    # Set array for only zone IDs
    zone_id_list = []
    
    # Call looped zone ids pages function
    raw_zone_ids = loop_zone_id_pages(BASE_URL, headers)
    
    # Iterate raw list for zone_ids
    for zone_ids in raw_zone_ids:
        
        # Get each zone ID from list
        zone_id = zone_ids["id"]
        
        zone_id_list.append(zone_id)

    return zone_id_list

def delete_rules_current_ruleset(BASE_URL, headers, phase):
    zone_ids = iterate_zone_ids_into_list(BASE_URL, headers)
    
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