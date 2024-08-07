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
    raw_zone_id_list = []
    page = 1
    while True:
        response = requests.get(BASE_URL+ f"?page={page}&per_page=1000", headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve data from the Zone IDs API. Status code: {response.status_code}")
        
        data = response.json()
        raw_zone_ids = data["result"]
        raw_zone_id_list.extend(raw_zone_ids)
        
        if not raw_zone_ids:
            break
        page += 1
    return raw_zone_id_list

def iterate_zone_ids_into_list(BASE_URL, headers):
    zone_id_list = []
    raw_zone_ids = loop_zone_id_pages(BASE_URL, headers)
    
    for zone_data in raw_zone_ids:
        zone_id_list.append({"id": zone_data["id"], "name": zone_data["name"]})
    return zone_id_list

def list_custom_hostnames(BASE_URL, headers, zone_id, zone_name):
    custom_hostnames_list = []
    page = 1
    while True:
        custom_hostnames_api = BASE_URL + f"/{zone_id}/custom_hostnames?page={page}&per_page=1000"
        response = requests.get(custom_hostnames_api, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve data from the Custom Hostnames API. Status code: {response.status_code}")
        
        data = response.json()
        raw_custom_hostnames = data["result"]
        
        for hostname in raw_custom_hostnames:
            custom_hostname = {
                "id": hostname["id"],
                "hostname": hostname["hostname"],
                "status": hostname["status"],
                "custom_origin_server": hostname.get("custom_origin_server", "null"),
                "custom_origin_sni": hostname.get("custom_origin_sni", "null"),
                "ssl_status": hostname["ssl"]["status"],
                "ssl_method": hostname["ssl"]["method"],
                "ssl_type": hostname["ssl"]["type"],
                "created_at": hostname["created_at"],
                "zone_id": zone_id,
                "zone_name": zone_name
            }
            
            if "verification_errors" in hostname:
                custom_hostname["verification_errors"] = hostname["verification_errors"]
            
            custom_hostnames_list.append(custom_hostname)
        
        result_info = data["result_info"]
        if result_info["page"] >= result_info["total_pages"]:
            break
        else:
            page += 1
    
    return custom_hostnames_list

# Main execution
if __name__ == "__main__":
    zone_data = iterate_zone_ids_into_list(BASE_URL, headers)

    all_custom_hostnames = []
    for zone in zone_data:
        custom_hostnames = list_custom_hostnames(BASE_URL, headers, zone["id"], zone["name"])
        all_custom_hostnames.extend(custom_hostnames)

    # Prepare the final output
    output = {
        "total_count": len(all_custom_hostnames),
        "custom_hostnames": all_custom_hostnames
    }

    # Print the results
    print(json.dumps(output, indent=2))
