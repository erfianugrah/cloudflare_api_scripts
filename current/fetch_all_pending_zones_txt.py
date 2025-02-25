import os
import requests
import json
import asyncio
import aiohttp
import csv
import argparse
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

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Fetch Cloudflare pending zones and verification details')
    
    # Account filtering options
    account_group = parser.add_mutually_exclusive_group()
    account_group.add_argument('--account', '-a', help='Process a single account by ID')
    account_group.add_argument('--accounts-file', '-f', help='File containing account IDs to process (one per line)')
    
    # Output options
    parser.add_argument('--output-prefix', '-o', help='Prefix for output filenames')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv',
                        help='Output format for verification records (default: csv)')
    
    return parser.parse_args()

def load_account_ids_from_file(file_path: str) -> List[str]:
    """Load account IDs from a file, one per line"""
    account_ids = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                account_id = line.strip()
                if account_id:  # Skip empty lines
                    account_ids.append(account_id)
        return account_ids
    except Exception as e:
        print(f"Error loading account IDs from file: {str(e)}")
        return []

async def process_zone(session, zone, records):
    """Process a zone to get verification data"""
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
        
        # Add record
        records.append({
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
                        
                        records.append({
                            'Zone Name': zone_name,
                            'Status': status,
                            'Account Name': account_name,
                            'Verification Type': 'DCV',
                            'Record Name': actual_record_name,
                            'Record Type': 'CNAME',
                            'Record Value': actual_record_value
                        })
            else:
                # No DNS records found
                records.append({
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
            records.append({
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
    args = parse_arguments()
    
    # Validate authentication credentials
    if not (auth_token or (auth_email and auth_key)):
        print("Error: No authentication credentials found.")
        print("Please set CLOUDFLARE_API_TOKEN or both CLOUDFLARE_API_KEY and CLOUDFLARE_EMAIL.")
        return
    
    # Show authentication method
    if auth_token:
        print("Using API Token for authentication")
    else:
        print(f"Using API Key + Email for authentication ({auth_email})")
    
    # Set up output filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = args.output_prefix or "pending_zones"
    output_format = args.format.lower()
    output_filename = f"{prefix}_{timestamp}.{output_format}"
    raw_data_filename = f"{prefix}_raw_{timestamp}.json"
    
    # Initialize data
    verification_records = []
    results = {
        "pending_zones": [],
        "total_pending": 0,
        "accounts_with_pending": 0,
        "accounts_checked": 0
    }
    
    # Setup CSV file if needed
    csvfile = None
    writer = None
    if output_format == "csv":
        csvfile = open(output_filename, 'w', newline='')
        writer = csv.DictWriter(csvfile, fieldnames=[
            'Zone Name', 'Status', 'Account Name', 'Verification Type', 
            'Record Name', 'Record Type', 'Record Value'
        ])
        writer.writeheader()
    
    # Process accounts and zones
    try:
        async with aiohttp.ClientSession() as session:
            # Get accounts
            all_accounts = await get_accounts(session)
            
            # Filter accounts if specified
            if args.account:
                # Single account specified
                accounts = [a for a in all_accounts if a.get("id") == args.account]
                print(f"Filtered to account: {args.account}")
            elif args.accounts_file:
                # Multiple accounts from file
                account_ids = load_account_ids_from_file(args.accounts_file)
                accounts = [a for a in all_accounts if a.get("id") in account_ids]
                print(f"Loaded {len(account_ids)} account IDs from {args.accounts_file}")
            else:
                # All accounts
                accounts = all_accounts
                print(f"Processing all {len(accounts)} accounts")
            
            # Update results
            results["accounts_checked"] = len(accounts)
            
            # Process each account
            for account in accounts:
                account_id = account.get("id")
                account_name = account.get("name")
                
                if not account_id:
                    continue
                
                print(f"Processing account: {account_name} ({account_id})")
                
                # Get zones for this account
                zones = await get_zones_for_account(session, account_id)
                print(f"Found {len(zones)} zones in account {account_name}")
                
                # Filter for pending zones
                pending_zones = []
                for zone in zones:
                    status = zone.get("status", "")
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
                            verification = await process_zone(session, zone, verification_records)
                            zone["verification"] = verification
                            results["pending_zones"].append(zone)
                        except Exception as e:
                            print(f"  Error processing zone {zone['name']}: {str(e)}")
                else:
                    print(f"No pending zones found in account {account_name}")
    
        # Write CSV data if needed
        if output_format == "csv" and csvfile:
            for record in verification_records:
                writer.writerow(record)
            csvfile.close()
            print(f"CSV export saved to: {output_filename}")
        
        # Write JSON data if needed
        if output_format == "json":
            with open(output_filename, "w") as f:
                json.dump({"verification_records": verification_records}, f, indent=2)
            print(f"JSON export saved to: {output_filename}")
        
        # Always save raw data
        with open(raw_data_filename, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Raw data saved to: {raw_data_filename}")
        
        # Print summary
        print("\nSummary:")
        print(f"Total accounts checked: {results['accounts_checked']}")
        print(f"Accounts with pending zones: {results['accounts_with_pending']}")
        print(f"Total pending zones: {results['total_pending']}")
        
        # Print verification details if any
        if results["pending_zones"]:
            print_verification_details(results["pending_zones"])
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        if csvfile:
            csvfile.close()

def print_verification_details(pending_zones):
    """Print verification details in an easy-to-copy format"""
    verification_found = False
    
    # Show TXT verification values
    print("\nTXT Verification Records:")
    print("-------------------------")
    for zone in pending_zones:
        if "txt_verification" in zone.get("verification", {}):
            verification_found = True
            txt_info = zone["verification"]["txt_verification"]
            print(f"Zone: {zone['name']} ({zone['status']})")
            record_name = txt_info.get("record_name", "cloudflare-verify")
            print(f"TXT Record: {record_name}.{zone['name']}")
            print(f"Value: {txt_info['record_value']}")
            print()
    
    # Show DCV delegation instructions
    print("\nDCV Delegation for Partial Zones:")
    print("------------------------------------")
    for zone in pending_zones:
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

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"\nError: {str(e)}")
