import os
import sys
import argparse
import requests

# Base endpoint for Cloudflare's API for zones
BASE_URL = "https://api.cloudflare.com/client/v4/zones"

def setup_auth(api_token=None, api_key=None, email=None):
    """Setup authentication headers from args or environment"""
    if api_token:
        return {"Authorization": f"Bearer {api_token}"}
    elif api_key and email:
        return {
            "X-Auth-Key": api_key,
            "X-Auth-Email": email
        }
    else:
        # Try environment variables
        api_token = os.environ.get('CLOUDFLARE_API_TOKEN')
        if api_token:
            return {"Authorization": f"Bearer {api_token}"}
        
        api_key = os.environ.get('CLOUDFLARE_API_KEY')
        email = os.environ.get('CLOUDFLARE_EMAIL')
        if api_key and email:
            return {
                "X-Auth-Key": api_key,
                "X-Auth-Email": email
            }
    
    print("Error: Please provide authentication via arguments or environment variables")
    sys.exit(1)

def get_user_zone_ids():
    user_input = input("Enter the zone IDs, separated by commas: ")
    return [zone_id.strip() for zone_id in user_input.split(',')]

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
        print(f"Warning: Failed to delete DNS record {record_id}. Status code: {response.status_code}")
        print(response.text)
        return
    print(f"Deleted DNS record {record_id} successfully.")

def main():
    parser = argparse.ArgumentParser(description='Delete Cloudflare DNS records')
    parser.add_argument('--zone-ids', help='Comma-separated list of zone IDs')
    parser.add_argument('--api-token', help='Cloudflare API token')
    parser.add_argument('--api-key', help='Cloudflare API key')
    parser.add_argument('--email', help='Cloudflare email')

    args = parser.parse_args()

    # Setup authentication
    headers = setup_auth(args.api_token, args.api_key, args.email)

    # Get zone IDs from args or prompt
    zone_ids = [id.strip() for id in args.zone_ids.split(',')] if args.zone_ids else get_user_zone_ids()

    # Process each zone
    for zone_id in zone_ids:
        # Get records first to show count
        dns_records = list_all_dns_records(BASE_URL, headers, zone_id)
        
        # Show record count and confirm
        print(f"\nFound {len(dns_records)} records in zone {zone_id}")
        confirm = input("Do you want to proceed with deletion? [y/N]: ")
        
        if confirm.lower() != 'y':
            print("Skipping zone", zone_id)
            continue

        # Delete records
        for record in dns_records:
            delete_dns_record(BASE_URL, headers, zone_id, record["id"])

if __name__ == "__main__":
    main()
