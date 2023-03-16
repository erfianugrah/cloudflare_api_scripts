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

def delete_rules_in_ruleset(BASE_URL, headers, phase):
    zone_ids = iterate_zone_ids_into_list(BASE_URL, headers)

    # Get the ID from the current item
    for zone_id in zone_ids:

        rulesets_api = BASE_URL + f"/{zone_id}/rulesets"
        response = requests.get(rulesets_api, headers=headers)
        data = response.json()
        
        # Iterate over the data from the current page of the rulesets API
        for rulesets_ids in data["result"]:
            if rulesets_ids["phase"] == phase:
                ruleset_id = rulesets_ids["id"]
                
                # Get version objects
                rulesets_version_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}/versions"
                response = requests.get(rulesets_version_api, headers=headers)
                data = response.json()
            
                # Iterate over the version ids and skipping the latest one, which is usually in the first object, since you can't delete the latest version as it is the same as the pinned version at this point         
                for rulesets_version_id in data["result"][1:]:
                    for version_id in rulesets_version_id["version"]:
                        version_id = rulesets_version_id["version"]
                    
                    # Nuke it
                    rulesets_specific_versions_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}/versions/{version_id}"
                    response = requests.delete(rulesets_specific_versions_api, headers=headers)
                    print(response.text)
                    
    return response
                    
delete_rules_in_ruleset(BASE_URL, headers, phase)