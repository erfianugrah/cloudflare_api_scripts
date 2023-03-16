import os
import requests
import json

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

# Initiate list for "for loop"
zone_ids = iterate_zone_ids_into_list(BASE_URL, headers)

def list_dns_records_proxied(BASE_URL, headers, zone_id):
    # Set array of list of proxied DNs records
    proxied_dns_records_list = []
    page = 1
    while True:
        dns_records_api = BASE_URL + f"/{zone_id}/dns_records?page={page}&per_page=1000"
        response = requests.get(dns_records_api, headers=headers)
        
        # Catch error
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve data from the DNS Records API. Status code: {response.status_code}")
        
        # Parse the response as JSON
        data = response.json()
        
        # Get the result payload
        raw_dns_records = data["result"]

        for dns_records in raw_dns_records:
            proxied_dns_record = {}
            if dns_records["proxied"]:
                proxied_dns_record["name"] = dns_records["name"]
                proxied_dns_record["type"] = dns_records["type"]
                proxied_dns_record["content"] = dns_records["content"]
                proxied_dns_record["ttl"] = dns_records["ttl"]
                proxied_dns_record["zone_name"] = dns_records["zone_name"]
                
            # Add all dns_records into list
            if proxied_dns_record:
                proxied_dns_records_list.append(proxied_dns_record)
            
        if not raw_dns_records:
            break
        else:
            page += 1
        
    return proxied_dns_records_list

# Specify the filename to write the results to
filename = 'dns_records.json'

# Open the file in write mode
with open(filename, 'w') as f:
    
    # Loop through the list of zone IDs
    for zone_id in zone_ids:
        
        # Call the list_dns_records_proxied() function to get the DNS records
        dns_records = list_dns_records_proxied(BASE_URL, headers, zone_id)
        
        if dns_records:
        # Write the DNS records to the file, one zone at a time
            f.write(json.dumps({zone_id: dns_records}) + '\n')