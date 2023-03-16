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

filter = input("Enter filter description: ")

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

def delete_all_filters_by_ip_list(BASE_URL, headers, filter):
    zone_ids = iterate_zone_ids_into_list(BASE_URL, headers)
    # Iterate over the data from the first API
    for zone_id in zone_ids:

        # Make a request to the second API endpoint for the current ID
        page = 1
        while True:
            # Get filters
            filters_api = BASE_URL + f"/{zone_id}/filters?page={page}&per_page=1000"
            response = requests.get(filters_api, headers=headers)
            data = response.json()
            
            # Iterate over the data from the current page of the filters API
            for filters in data["result"]:

                # Check if the filter matches the expression 
                if filter in filters["expression"]:
                    
                    # Get the ID from the current item
                    filters_id = filters["id"]
                    
                    # Make a request to the filters API endpoint for the current IDs
                    filters_id_api = BASE_URL + f"/{zone_id}/filters/{filters_id}"
                    response = requests.get(filters_id_api, headers=headers)
                    print(response.text)

            # Check if there are more pages of results
            if not data["result"]:
                break
            # Move to the next page of results
            page += 1
            
    return response

delete_all_filters_by_ip_list(BASE_URL, headers, filter)