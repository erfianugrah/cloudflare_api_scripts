import os
import requests
import json
import asyncio
import aiohttp
import csv
from datetime import datetime
from typing import List, Dict, Any

# Base endpoint for Cloudflare's API
ZONES_BASE_URL = "https://api.cloudflare.com/client/v4/zones"
ACCOUNTS_BASE_URL = "https://api.cloudflare.com/client/v4/accounts"

# Define Auth headers
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')
auth_token = os.environ.get('CLOUDFLARE_API_TOKEN')

# Set headers based on available credentials
if auth_token:
    headers = {"Authorization": f"Bearer {auth_token}"}
else:
    headers = {
        "X-Auth-Key": auth_key,
        "X-Auth-Email": auth_email
    }

async def fetch_data(session: aiohttp.ClientSession, url: str) -> Dict:
    """Make an asynchronous API request to Cloudflare"""
    try:
        async with session.get(url, headers=headers) as response:
            response_text = await response.text()
            
            if response.status != 200:
                print(f"URL: {url}")
                print(f"Response status: {response.status}")
                print(f"Response body: {response_text}")
                return {"success": False, "result": None}
                
            try:
                return json.loads(response_text)
            except json.JSONDecodeError:
                print(f"Failed to parse JSON response from {url}")
                return {"success": False, "result": None}
                
    except Exception as e:
        print(f"Error fetching data from {url}: {str(e)}")
        return {"success": False, "result": None}

async def get_accounts(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Get all accounts accessible with the API credentials"""
    print("Fetching accounts...")
    data = await fetch_data(session, ACCOUNTS_BASE_URL)
    accounts = data.get("result", [])
    print(f"Found {len(accounts)} accounts")
    return accounts

async def get_zones_for_account(session: aiohttp.ClientSession, account_id: str) -> List[Dict[str, Any]]:
    """Get all zones for a specific account"""
    zones = []
    page = 1
    
    # The account-level zones endpoint isn't working, so we'll use the main zones endpoint
    # and filter by account_id
    while True:
        url = f"{ZONES_BASE_URL}?page={page}&per_page=1000&account.id={account_id}"
        data = await fetch_data(session, url)
        
        if not data.get("success", False):
            break
        
        batch = data.get("result", [])
        zones.extend(batch)
        
        # Check if we've reached the last page
        if len(batch) < 1000:
            break
        
        page += 1
    
    return zones

async def get_dns_records(session: aiohttp.ClientSession, zone_id: str) -> List[Dict[str, Any]]:
    """Get all DNS records for a zone (A, AAAA, CNAME)"""
    dns_records = []
    
    # Get A records
    a_url = f"{ZONES_BASE_URL}/{zone_id}/dns_records?type=A"
    a_response = await fetch_data(session, a_url)
    if a_response.get("success"):
        dns_records.extend(a_response.get("result", []))
        
    # Get AAAA records
    aaaa_url = f"{ZONES_BASE_URL}/{zone_id}/dns_records?type=AAAA"
    aaaa_response = await fetch_data(session, aaaa_url)
    if aaaa_response.get("success"):
        dns_records.extend(aaaa_response.get("result", []))
        
    # Get CNAME records
    cname_url = f"{ZONES_BASE_URL}/{zone_id}/dns_records?type=CNAME"
    cname_response = await fetch_data(session, cname_url)
    if cname_response.get("success"):
        dns_records.extend(cname_response.get("result", []))
        
    return dns_records

async def get_txt_verification(session: aiohttp.ClientSession, zone_id: str, zone_name: str) -> Dict[str, Any]:
    """Get TXT verification details for a zone"""
    txt_verification = None
    
    # Try zone details endpoint for verification_key
    zone_url = f"{ZONES_BASE_URL}/{zone_id}"
    zone_response = await fetch_data(session, zone_url)
    
    if zone_response.get("success"):
        zone_details = zone_response.get("result", {})
        
        # Check for verification_key in the zone details
        verification_key = zone_details.get("verification_key")
        if verification_key:
            txt_verification = {
                "record_name": "cloudflare-verify",
                "record_type": "TXT",
                "record_value": verification_key,
                "source": "zone_details.verification_key"
            }
            print(f"  Found verification key in zone details: {verification_key}")
    
    return txt_verification

async def get_dcv_delegation(session: aiohttp.ClientSession, zone_id: str) -> Dict[str, Any]:
    """Get DCV delegation UUID for a zone"""
    dcv_delegation = None
    
    # Try the dcv_delegation endpoint
    dcv_url = f"{ZONES_BASE_URL}/{zone_id}/dcv_delegation/uuid"
    dcv_response = await fetch_data(session, dcv_url)
    
    if dcv_response.get("success"):
        dcv_result = dcv_response.get("result", {})
        uuid = dcv_result.get("uuid")
        
        if uuid:
            dcv_delegation = {
                "record_format": f"_acme-challenge.<hostname> CNAME <hostname>.{uuid}.dcv.cloudflare.com",
                "uuid": uuid,
                "source": "dcv_delegation_uuid"
            }
            print(f"  Found DCV delegation UUID: {uuid}")
    
    return dcv_delegation

async def process_zone(session: aiohttp.ClientSession, zone: Dict[str, Any], csv_writer) -> Dict[str, Any]:
    """Process a zone to get all verification data"""
    zone_id = zone["id"]
    zone_name = zone["name"]
    account_name = zone["account_name"]
    status = zone["status"]
    
    # Initialize verification data
    verification = {}
    
    # Get TXT verification
    txt_verification = await get_txt_verification(session, zone_id, zone_name)
    if txt_verification:
        verification["txt_verification"] = txt_verification
        
        # Add to CSV
        csv_writer.writerow({
            'Zone Name': zone_name,
            'Status': status,
            'Account Name': account_name,
            'Verification Type': 'TXT',
            'Record Name': f"{txt_verification['record_name']}.{zone_name}",
            'Record Type': txt_verification['record_type'],
            'Record Value': txt_verification['record_value']
        })
    
    # Get DCV delegation
    dcv_delegation = await get_dcv_delegation(session, zone_id)
    if dcv_delegation:
        verification["dcv_delegation"] = dcv_delegation
        
        # Get DNS records
        try:
            dns_records = await get_dns_records(session, zone_id)
            if dns_records:
                verification["dns_records"] = dns_records
                print(f"  Found {len(dns_records)} A/AAAA/CNAME records")
                
                # Add entries for each hostname
                for record in dns_records:
                    if record["type"] in ["A", "AAAA", "CNAME"]:
                        # Extract the hostname part
                        hostname = record["name"]
                        if hostname.endswith(f".{zone_name}"):
                            hostname = hostname[:-len(zone_name)-1]  # Remove zone part
                        if not hostname:
                            hostname = "@"  # Root domain
                        
                        # Create the actual DCV delegation record with real hostname
                        actual_record_name = f"_acme-challenge.{hostname}"
                        actual_record_value = f"{hostname}.{dcv_delegation['uuid']}.dcv.cloudflare.com"
                        if hostname == "@":
                            actual_record_value = f"{zone_name}.{dcv_delegation['uuid']}.dcv.cloudflare.com"
                            
                        csv_writer.writerow({
                            'Zone Name': zone_name,
                            'Status': status,
                            'Account Name': account_name,
                            'Verification Type': 'DCV',
                            'Record Name': actual_record_name,
                            'Record Type': 'CNAME',
                            'Record Value': actual_record_value
                        })
            else:
                # If no DNS records found, indicate that in the CSV
                csv_writer.writerow({
                    'Zone Name': zone_name,
                    'Status': status,
                    'Account Name': account_name,
                    'Verification Type': 'DCV',
                    'Record Name': 'No records found',
                    'Record Type': 'INFO',
                    'Record Value': f'No records to verify with DCV delegation: {dcv_delegation["uuid"]}'
                })
        except Exception as e:
            print(f"  Error fetching DNS records: {str(e)}")
            # Add error entry if exception occurred
            csv_writer.writerow({
                'Zone Name': zone_name,
                'Status': status,
                'Account Name': account_name,
                'Verification Type': 'DCV',
                'Record Name': 'Error',
                'Record Type': 'ERROR',
                'Record Value': f'Error retrieving records: {str(e)}'
            })
    
    return verification

async def main():
    """Main execution function"""
    results = {
        "pending_zones": [],
        "total_pending": 0,
        "accounts_with_pending": 0
    }
    
    # Check for authentication credentials
    if not (auth_token or (auth_email and auth_key)):
        print("Error: No authentication credentials found.")
        print("Please set one of the following environment variables:")
        print("  - CLOUDFLARE_API_TOKEN")
        print("  - CLOUDFLARE_API_KEY and CLOUDFLARE_EMAIL")
        return
    
    # Print authentication method being used
    if auth_token:
        print("Using API Token for authentication")
    else:
        print(f"Using API Key + Email for authentication ({auth_email})")
    
    # CSV preparation
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"pending_zones_{timestamp}.csv"
    json_filename = f"pending_zones_{timestamp}.json"
    
    # Open CSV file and prepare writer
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['Zone Name', 'Status', 'Account Name', 'Verification Type', 
                      'Record Name', 'Record Type', 'Record Value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        async with aiohttp.ClientSession() as session:
            # Get all accounts
            accounts = await get_accounts(session)
            
            for account in accounts:
                account_id = account.get("id")
                account_name = account.get("name")
                
                # Skip if account information is incomplete
                if not account_id:
                    continue
                
                print(f"Processing account: {account_name} ({account_id})")
                
                # Get zones for this account
                zones = await get_zones_for_account(session, account_id)
                
                # Log how many zones were found
                print(f"Found {len(zones)} zones in account {account_name}")
                
                # Filter for pending zones
                pending_zones = []
                for zone in zones:
                    status = zone.get("status", "")
                    # Include zones with pending status
                    if status != "active":
                        pending_zones.append({
                            "id": zone["id"],
                            "name": zone["name"],
                            "status": status,
                            "account_id": account_id,
                            "account_name": account_name,
                            "type": zone.get("type", "unknown")
                        })
                
                if pending_zones:
                    print(f"Found {len(pending_zones)} pending zones in account {account_name}")
                    results["accounts_with_pending"] += 1
                    results["total_pending"] += len(pending_zones)
                    
                    # Process each pending zone
                    for zone in pending_zones:
                        try:
                            verification = await process_zone(session, zone, writer)
                            zone["verification"] = verification
                            results["pending_zones"].append(zone)
                        except Exception as e:
                            print(f"  Error processing zone {zone['name']}: {str(e)}")
                else:
                    print(f"No pending zones found in account {account_name}")
    
    # Save results to JSON
    try:
        with open(json_filename, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to: {json_filename}")
    except Exception as e:
        print(f"Error saving results to JSON file: {str(e)}")
    
    print(f"CSV export saved to: {csv_filename}")
    
    # Print summary
    print("\nSummary:")
    print(f"Total accounts checked: {len(accounts)}")
    print(f"Accounts with pending zones: {results['accounts_with_pending']}")
    print(f"Total pending zones: {results['total_pending']}")
    
    # Output verification records in an easy-to-copy format
    if results["pending_zones"]:
        verification_found = False
        
        # First show TXT verification values
        print("\nTXT Verification Records:")
        print("-------------------------")
        for zone in results["pending_zones"]:
            if "txt_verification" in zone.get("verification", {}):
                verification_found = True
                txt_info = zone["verification"]["txt_verification"]
                print(f"Zone: {zone['name']} ({zone['status']})")
                record_name = txt_info.get("record_name", "cloudflare-verify")
                print(f"TXT Record: {record_name}.{zone['name']}")
                print(f"Value: {txt_info['record_value']}")
                print()
        
        # Then show DCV delegation instructions
        print("\nDCV Delegation for Partial Zones:")
        print("------------------------------------")
        for zone in results["pending_zones"]:
            if "dcv_delegation" in zone.get("verification", {}):
                verification_found = True
                dcv_info = zone["verification"]["dcv_delegation"]
                uuid = dcv_info["uuid"]
                print(f"Zone: {zone['name']} ({zone['status']})")
                
                # List all A, AAAA, CNAME records that need DCV delegation
                dns_records = zone["verification"].get("dns_records", [])
                if dns_records:
                    print("\nRequired DCV delegation records:")
                    for record in dns_records:
                        if record["type"] in ["A", "AAAA", "CNAME"]:
                            # Extract the hostname part
                            hostname = record["name"]
                            if hostname.endswith(f".{zone['name']}"):
                                hostname = hostname[:-len(zone['name'])-1]  # Remove zone part
                            if not hostname:
                                hostname = "@"  # Root domain
                            
                            # Create the actual DCV delegation record with real hostname
                            actual_record_name = f"_acme-challenge.{hostname}"
                            actual_record_value = f"{hostname}.{uuid}.dcv.cloudflare.com"
                            if hostname == "@":
                                actual_record_value = f"{zone['name']}.{uuid}.dcv.cloudflare.com"
                                
                            print(f"  {actual_record_name} CNAME {actual_record_value}")
                else:
                    print("  No hostnames found to verify with DCV delegation")
                
                print()
        
        if not verification_found:
            print("No verification values found for any zones.")
            print("You may need to initiate zone verification from the Cloudflare dashboard.")
            print("After initiating verification, run this script again to retrieve the values.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"\nError: {str(e)}")
