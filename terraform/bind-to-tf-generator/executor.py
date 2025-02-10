import os
import sys
import argparse
import requests
from typing import List, Dict
from bindgenerator import main as bindgen_main

def get_zones() -> List[Dict]:
    """Get all zones from Cloudflare account"""
    base_url = "https://api.cloudflare.com/client/v4/zones"
    
    # Get auth from environment
    api_token = os.environ.get('CLOUDFLARE_API_TOKEN')
    api_key = os.environ.get('CLOUDFLARE_API_KEY')
    email = os.environ.get('CLOUDFLARE_EMAIL')
    
    if api_token:
        headers = {"Authorization": f"Bearer {api_token}"}
    elif api_key and email:
        headers = {
            "X-Auth-Key": api_key,
            "X-Auth-Email": email
        }
    else:
        raise ValueError("Set either CLOUDFLARE_API_TOKEN or both CLOUDFLARE_API_KEY and CLOUDFLARE_EMAIL")

    zones = []
    page = 1
    
    while True:
        response = requests.get(f"{base_url}?page={page}&per_page=1000", headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to get zones. Status: {response.status_code}\n{response.text}")
            
        data = response.json()
        results = data["result"]
        if not results:
            break
            
        zones.extend(results)
        page += 1
    
    return zones

def export_dns_records(zone_id: str) -> str:
    """Export DNS records for a zone in BIND format"""
    base_url = f"https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records/export"
    
    api_token = os.environ.get('CLOUDFLARE_API_TOKEN')
    api_key = os.environ.get('CLOUDFLARE_API_KEY')
    email = os.environ.get('CLOUDFLARE_EMAIL')
    
    if api_token:
        headers = {"Authorization": f"Bearer {api_token}"}
    elif api_key and email:
        headers = {
            "X-Auth-Key": api_key,
            "X-Auth-Email": email
        }
    else:
        raise ValueError("Set either CLOUDFLARE_API_TOKEN or both CLOUDFLARE_API_KEY and CLOUDFLARE_EMAIL")

    response = requests.get(base_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to export DNS records. Status: {response.status_code}\n{response.text}")
        
    return response.text

def main():
    parser = argparse.ArgumentParser(description='Cloudflare DNS Export and Migration Tool')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--list-zones', action='store_true', help='List all zones in account')
    group.add_argument('--export-dns', metavar='ZONE_ID', help='Export DNS records for zone ID')
    
    # Capture remaining args to pass to bindgenerator
    parser.add_argument('remaining_args', nargs=argparse.REMAINDER, 
                       help='Additional arguments to pass to bindgenerator')

    args = parser.parse_args()

    try:
        if args.list_zones or not (args.export_dns or args.remaining_args):
            # List all zones
            zones = get_zones()
            print("\nAvailable zones:")
            for zone in zones:
                print(f"ID: {zone['id']:<32} Name: {zone['name']}")
            return

        if args.export_dns:
            # Export DNS records to BIND format
            print(f"Exporting DNS records for zone {args.export_dns}...")
            bind_content = export_dns_records(args.export_dns)
            
            # Save to file
            bind_file = f"zone_{args.export_dns}.bind"
            with open(bind_file, 'w') as f:
                f.write(bind_content)
            print(f"DNS records exported to {bind_file}")

            # If there are remaining args, pass to bindgenerator
            if args.remaining_args:
                sys.argv = ['bindgenerator.py'] + args.remaining_args
                bindgen_main()
        
        elif args.remaining_args:
            # Pass any other commands directly to bindgenerator
            sys.argv = ['bindgenerator.py'] + args.remaining_args
            bindgen_main()

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
