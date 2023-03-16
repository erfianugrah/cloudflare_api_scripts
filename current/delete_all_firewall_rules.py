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
      
def delete_firewall_rules(BASE_URL, headers):
    zone_ids = iterate_zone_ids_into_list(BASE_URL, headers)
    for zone_id in zone_ids:

        # Make a request to the second API endpoint for the current ID
        page = 1
        while True:
            firewall_rules_api = BASE_URL + f"/{zone_id}/firewall/rules?page={page}&per_page=1000"
            response = requests.get(firewall_rules_api, headers=headers)
            data = response.json()
            
            # Iterate over the data from the current page of the second API
            for firewall_rule_ids in data["result"]:
                    
                # Get the ID from the current item
                firewall_rule_id = firewall_rule_ids["id"]

                # Make a request to the third API endpoint for the current IDs
                firewall_rules_id_api = BASE_URL + f"/{zone_id}/firewall/rules/{firewall_rule_id}"
                response = requests.delete(firewall_rules_id_api, headers=headers)
                print(response.text)
                
            # Check if there are more pages of results
            if not data["result"]:
                break

            # Move to the next page of results
            page += 1
            
    return response

delete_firewall_rules(BASE_URL, headers)