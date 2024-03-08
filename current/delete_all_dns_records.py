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

def get_user_zone_ids():
    # Prompt the user to enter zone IDs, separated by commas
    user_input = input("Enter the zone IDs, separated by commas: ")
    # Split the input string into a list of zone IDs
    zone_ids = [zone_id.strip() for zone_id in user_input.split(',')]
    return zone_ids

def list_all_dns_records(BASE_URL, headers, zone_id):
    all_dns_records_list = []
    page = 1
    while True:
        dns_records_api = BASE_URL + f"/{zone_id}/dns_records?page={page}&per_page=1000"
        response = requests.get(dns_records_api, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve data from the DNS Records API. Status code: {response.status_code}")
        data = response.json()
        raw_dns_records = data["result"]
        all_dns_records_list.extend(raw_dns_records)
        if not raw_dns_records:
            break
        else:
            page += 1
    return all_dns_records_list

def delete_dns_record(BASE_URL, headers, zone_id, record_id):
    delete_api = BASE_URL + f"/{zone_id}/dns_records/{record_id}"
    response = requests.delete(delete_api, headers=headers)
    if response.status_code != 200:
        # Print response body for more detailed error information
        print(f"Warning: Failed to delete DNS record {record_id}. Status code: {response.status_code}")
        print(response.text)
        # Continue with the next record instead of raising an exception
        return
    print(f"Deleted DNS record {record_id} successfully.")



# Get zone IDs from the user
zone_ids = get_user_zone_ids()

# Loop through the list of user-supplied zone IDs
for zone_id in zone_ids:
    # Get all DNS records for the zone
    dns_records = list_all_dns_records(BASE_URL, headers, zone_id)
    # Loop through the DNS records and delete them
    for record in dns_records:
        delete_dns_record(BASE_URL, headers, zone_id, record["id"])
