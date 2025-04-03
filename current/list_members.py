import asyncio
import aiohttp
import csv
import sys
import os
import argparse
import json
from typing import List, Dict, Any
from datetime import datetime

# Parse command line arguments
parser = argparse.ArgumentParser(description='Export Cloudflare accounts and members to CSV and Markdown')
parser.add_argument('--token', help='Cloudflare API token')
parser.add_argument('--key', help='Cloudflare API key')
parser.add_argument('--email', help='Cloudflare account email (required if using API key)')
parser.add_argument('--output', default="cloudflare_accounts_members.csv", help='Output CSV file path')
parser.add_argument('--markdown', default="cloudflare_accounts_members.md", help='Output Markdown file path')
parser.add_argument('--debug', action='store_true', help='Enable debug output')
parser.add_argument('--verbose', action='store_true', help='Enable verbose debug output (more detailed than --debug)')
parser.add_argument('--show-token', action='store_true', help='Show full token in debug output (use with caution)')
parser.add_argument('--concurrent', type=int, default=10, help='Number of concurrent API requests (default: 10)')
args = parser.parse_args()

# Configuration
API_TOKEN = args.token
API_KEY = args.key
API_EMAIL = args.email
OUTPUT_FILE = args.output
MARKDOWN_FILE = args.markdown
DEBUG = args.debug
VERBOSE = args.verbose
SHOW_TOKEN = args.show_token
CONCURRENT_REQUESTS = args.concurrent

# API endpoints
ACCOUNTS_ENDPOINT = "https://api.cloudflare.com/client/v4/accounts"

# Set up headers for API requests
HEADERS = {"Content-Type": "application/json"}

# Debug logging function
def debug_log(message):
    if DEBUG:
        print(f"DEBUG: {message}")

def verbose_log(message):
    if VERBOSE:
        print(f"VERBOSE: {message}")

# Check if token length is valid
def validate_token(token):
    """Check if token appears to be valid"""
    if not token:
        return False
    
    # Most Cloudflare API tokens are longer than 40 characters
    if len(token) < 40:
        debug_log(f"Warning: API token length ({len(token)}) seems too short")
        return False
    
    return True

# Configure authentication method
def setup_auth():
    """Set up authentication headers based on available credentials"""
    global HEADERS
    
    debug_log("Setting up authentication")
    
    # Clear any existing auth headers to prevent conflicts
    for key in ["Authorization", "X-Auth-Key", "X-Auth-Email"]:
        if key in HEADERS:
            del HEADERS[key]
    
    # Try checking environment variables directly (without modifying with strip())
    env_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    env_key = os.environ.get("CLOUDFLARE_API_KEY")
    env_email = os.environ.get("CLOUDFLARE_EMAIL")
    
    # Try token-based auth first
    if API_TOKEN or env_token:
        # Token-based auth
        raw_token = API_TOKEN if API_TOKEN else env_token
        debug_log("Using API token for authentication")
        
        # Clean the token - remove any whitespace, quotes, etc.
        clean_token = raw_token.strip('"\' \t\n\r')
        if clean_token != raw_token:
            debug_log("Token was cleaned (contained whitespace or quotes)")
        
        if validate_token(clean_token):
            HEADERS["Authorization"] = f"Bearer {clean_token}"
            # Mask the actual token in debug output
            masked_token = f"{clean_token[:5]}...{clean_token[-5:]}" if len(clean_token) > 10 else "***"
            debug_log(f"Token (masked): {masked_token}")
            return True
        else:
            debug_log("API token validation failed - token may be invalid or truncated")
    
    # Try API key + email if token failed or not provided
    key = API_KEY if API_KEY else env_key
    email = API_EMAIL if API_EMAIL else env_email
    
    if key and email:
        debug_log("Using API key + email for authentication")
        clean_key = key.strip('"\' \t\n\r')
        clean_email = email.strip('"\' \t\n\r')
        
        HEADERS["X-Auth-Key"] = clean_key
        HEADERS["X-Auth-Email"] = clean_email
        debug_log(f"Email: {clean_email}")
        # Mask the actual key in debug output
        masked_key = f"{clean_key[:5]}...{clean_key[-5:]}" if len(clean_key) > 10 else "***"
        debug_log(f"Key (masked): {masked_key}")
        return True
    else:
        debug_log("No valid authentication credentials found")
    
    print("Error: No authentication credentials provided.")
    print("Please provide one of the following:")
    print("  - API Token via --token flag or CLOUDFLARE_API_TOKEN environment variable")
    print("  - API Key and Email via --key/--email flags or CLOUDFLARE_API_KEY/CLOUDFLARE_EMAIL environment variables")
    print("Note: Make sure your token/key is not truncated and doesn't contain extra whitespace or quotes.")
    return False

async def make_request(session, url: str) -> Dict[str, Any]:
    """Make an asynchronous API request to Cloudflare"""
    try:
        debug_log(f"Making request to {url}")
        if not SHOW_TOKEN:
            safe_headers = HEADERS.copy()
            if "Authorization" in safe_headers:
                token_part = safe_headers["Authorization"][:12]
                safe_headers["Authorization"] = f"{token_part}..."
            debug_log(f"Headers: {safe_headers}")
        else:
            debug_log(f"Headers: {HEADERS}")
        
        async with session.get(url, headers=HEADERS) as response:
            response_text = await response.text()
            
            if response.status != 200:
                debug_log(f"Response status code: {response.status}")
                debug_log(f"Response body: {response_text[:500]}...")  # Print first 500 chars of response
                raise Exception(f"API request failed: {response.status} {response.reason}")
            
            data = json.loads(response_text)
            if not data.get("success", False):
                error_msgs = ", ".join([err.get("message", "Unknown error") for err in data.get("errors", [])])
                raise Exception(f"Cloudflare API error: {error_msgs}")
            
            return data
    except aiohttp.ClientError as e:
        debug_log(f"Request exception: {str(e)}")
        raise Exception(f"Network error: {str(e)}")
    except json.JSONDecodeError as e:
        debug_log(f"JSON parsing error: {str(e)}")
        debug_log(f"Response content: {response_text[:500]}...")  # Print first 500 chars
        raise Exception("Failed to parse API response as JSON")

async def test_connectivity(session):
    """Test API connectivity"""
    # For token-based auth, we use the token verification endpoint
    if "Authorization" in HEADERS:
        test_url = "https://api.cloudflare.com/client/v4/user/tokens/verify"
    else:
        # For API key auth, we need a different endpoint that supports this auth method
        test_url = "https://api.cloudflare.com/client/v4/user"
    
    debug_log(f"Testing API connectivity using URL: {test_url}")
    
    try:
        async with session.get(test_url, headers=HEADERS) as response:
            response_text = await response.text()
            debug_log(f"API connectivity test response: {response.status}")
            
            if response.status == 200:
                debug_log(f"API connectivity test response body: {response_text}")
                return True
            else:
                debug_log(f"API connectivity test failed with status {response.status}")
                debug_log(f"Response body: {response_text}")
                print("API connectivity test failed. Your credentials may be invalid.")
                if '--show-token' not in sys.argv:
                    print("Try running with --show-token to see the full headers being sent.")
                return False
    except Exception as e:
        debug_log(f"API connectivity test failed: {str(e)}")
        return False

async def get_accounts(session) -> List[Dict[str, Any]]:
    """Fetch all accounts accessible with the API token with parallel pagination"""
    print("Fetching accounts...")
    accounts = []
    per_page = 50  # Maximum allowed value
    
    # Make initial request to get first page and total count
    url = f"{ACCOUNTS_ENDPOINT}?page=1&per_page={per_page}"
    initial_data = await make_request(session, url)
    
    # Add results from first page
    page_accounts = initial_data.get("result", [])
    accounts.extend(page_accounts)
    
    # Get pagination info from first page
    result_info = initial_data.get("result_info", {})
    total_count = result_info.get("total_count", 0)
    total_pages = (total_count + per_page - 1) // per_page
    
    debug_log(f"Accounts: Page 1, got {len(page_accounts)} accounts, total {total_count}, pages {total_pages}")
    
    # If we need more pages, fetch them in parallel
    if total_pages > 1:
        # Create tasks for remaining pages
        fetch_tasks = []
        for page in range(2, total_pages + 1):
            page_url = f"{ACCOUNTS_ENDPOINT}?page={page}&per_page={per_page}"
            task = asyncio.create_task(make_request(session, page_url))
            fetch_tasks.append((page, task))
        
        # Process results as they complete
        for page, task in fetch_tasks:
            try:
                data = await task
                page_accounts = data.get("result", [])
                accounts.extend(page_accounts)
                debug_log(f"Accounts: Page {page}, got {len(page_accounts)} accounts")
            except Exception as e:
                debug_log(f"Accounts: Error fetching page {page}: {e}")
    
    print(f"Found {len(accounts)} accounts (total expected: {total_count})")
    return accounts

async def get_account_members(session, account_id: str) -> Dict[str, Any]:
    """Fetch all members for a specific account with pagination and parallel requests"""
    members = []
    per_page = 50  # Maximum allowed value
    
    try:
        # Make initial request to get total count
        url = f"{ACCOUNTS_ENDPOINT}/{account_id}/members?page=1&per_page={per_page}"
        initial_data = await make_request(session, url)
        
        # Add results from first page
        page_members = initial_data.get("result", [])
        if page_members is None:
            verbose_log(f"Account {account_id}: First page returned null result")
            page_members = []
        members.extend(page_members)
        
        # Get pagination info from first page
        result_info = initial_data.get("result_info", {})
        if result_info is None:
            verbose_log(f"Account {account_id}: Missing result_info in response")
            result_info = {}
            
        total_count = result_info.get("total_count", 0)
        total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
        
        debug_log(f"Account {account_id}: Page 1, got {len(page_members)} members, total {total_count}, pages {total_pages}")
        
        # If we need more pages, fetch them in parallel
        if total_pages > 1:
            # Create tasks for remaining pages
            fetch_tasks = []
            for page in range(2, total_pages + 1):
                page_url = f"{ACCOUNTS_ENDPOINT}/{account_id}/members?page={page}&per_page={per_page}"
                task = asyncio.create_task(make_request(session, page_url))
                fetch_tasks.append((page, task))
            
            # Process results as they complete
            for page, task in fetch_tasks:
                try:
                    data = await task
                    page_members = data.get("result", [])
                    if page_members is None:
                        verbose_log(f"Account {account_id}: Page {page} returned null result")
                        page_members = []
                    members.extend(page_members)
                    debug_log(f"Account {account_id}: Page {page}, got {len(page_members)} members")
                except Exception as e:
                    debug_log(f"Account {account_id}: Error fetching page {page}: {e}")
        
        # Validate member objects
        valid_members = []
        for i, member in enumerate(members):
            if not isinstance(member, dict):
                verbose_log(f"Account {account_id}: Invalid member data at index {i}: {type(member)}")
                continue
            valid_members.append(member)
        
        if len(valid_members) != len(members):
            debug_log(f"Account {account_id}: Filtered out {len(members) - len(valid_members)} invalid member entries")
            members = valid_members
        
        print(f"Account {account_id}: Found {len(members)} members (total expected: {total_count})")
        return {"account_id": account_id, "members": members}
    except Exception as e:
        print(f"Warning: Failed to fetch members for account {account_id}: {e}")
        return {"account_id": account_id, "members": []}

async def get_all_members(session, accounts):
    """Fetch members for all accounts concurrently with rate limiting"""
    tasks = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async def bounded_fetch(account_id):
        async with semaphore:
            return await get_account_members(session, account_id)
    
    print(f"Fetching members for {len(accounts)} accounts (max {CONCURRENT_REQUESTS} concurrent requests)...")
    
    for account in accounts:
        account_id = account.get('id')
        if account_id:
            task = asyncio.create_task(bounded_fetch(account_id))
            tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    
    # Map results back to accounts
    accounts_map = {account.get('id'): account for account in accounts}
    for result in results:
        account_id = result.get('account_id')
        if account_id in accounts_map:
            accounts_map[account_id]['members'] = result.get('members', [])
    
    return list(accounts_map.values())

def write_to_csv(accounts_with_members: List[Dict[str, Any]]):
    """Write accounts and members data to CSV file"""
    print(f"Writing data to {OUTPUT_FILE}...")
    
    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        fieldnames = [
            'Account ID', 
            'Account Name', 
            'Member ID', 
            'Member Email', 
            'Member Status', 
            'Member Roles'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for account in accounts_with_members:
            account_id = account.get('id', '')
            account_name = account.get('name', '')
            
            members = account.get('members', [])
            
            # Validate members is a list
            if not isinstance(members, list):
                verbose_log(f"Account {account_id}: 'members' is not a list: {type(members)}")
                members = []
            
            if not members:
                # Account with no members
                writer.writerow({
                    'Account ID': account_id,
                    'Account Name': account_name,
                    'Member ID': '',
                    'Member Email': '',
                    'Member Status': '',
                    'Member Roles': ''
                })
            else:
                # Account with members
                for i, member in enumerate(members):
                    # Validate member is a dict
                    if not isinstance(member, dict):
                        verbose_log(f"Account {account_id}: Invalid member at index {i}: {type(member)}")
                        continue
                    
                    # Process roles safely
                    roles = []
                    member_roles = member.get('roles')
                    if member_roles is None:
                        verbose_log(f"Account {account_id}: Member at index {i} has null roles")
                        member_roles = []
                    elif not isinstance(member_roles, list):
                        verbose_log(f"Account {account_id}: Member at index {i} has invalid roles type: {type(member_roles)}")
                        member_roles = []
                    
                    # Iterate over roles safely with empty list fallback when None
                    for role in member_roles or []:
                        if isinstance(role, dict) and 'name' in role:
                            roles.append(role['name'])
                        else:
                            roles.append(str(role))
                    
                    # Process user data safely
                    user_data = member.get('user')
                    if user_data is None or not isinstance(user_data, dict):
                        verbose_log(f"Account {account_id}: Member at index {i} has invalid user data: {type(user_data)}")
                        user_data = {}
                    
                    writer.writerow({
                        'Account ID': account_id,
                        'Account Name': account_name,
                        'Member ID': member.get('id', ''),
                        'Member Email': user_data.get('email', ''),
                        'Member Status': member.get('status', ''),
                        'Member Roles': ', '.join(roles)
                    })
    
    print(f"CSV export complete: {OUTPUT_FILE}")

def write_to_markdown(accounts_with_members: List[Dict[str, Any]]):
    """Write accounts and members data to Markdown file"""
    print(f"Writing data to {MARKDOWN_FILE}...")
    
    with open(MARKDOWN_FILE, 'w') as mdfile:
        # Write header
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mdfile.write(f"# Cloudflare Accounts and Members Report\n\n")
        mdfile.write(f"Generated: {current_time}\n\n")
        
        # Write summary
        mdfile.write("## Summary\n\n")
        total_accounts = len(accounts_with_members)
        
        # Safely calculate total members
        total_members = 0
        for account in accounts_with_members:
            members = account.get('members')
            if isinstance(members, list):
                total_members += len(members)
            else:
                verbose_log(f"Account {account.get('id', 'unknown')}: members is not a list for total calculation")
        
        mdfile.write(f"- **Total Accounts**: {total_accounts}\n")
        mdfile.write(f"- **Total Members**: {total_members}\n")
        if total_accounts > 0:
            mdfile.write(f"- **Average Members per Account**: {total_members/total_accounts:.1f}\n\n")
        else:
            mdfile.write(f"- **Average Members per Account**: 0\n\n")
        
        # Create accounts table
        mdfile.write("## Accounts Overview\n\n")
        mdfile.write("| Account ID | Account Name | Member Count |\n")
        mdfile.write("|------------|--------------|-------------|\n")
        
        for account in sorted(accounts_with_members, key=lambda x: x.get('name', '')):
            account_id = account.get('id', '')
            account_name = account.get('name', '')
            
            # Safely calculate member count
            members = account.get('members')
            if isinstance(members, list):
                member_count = len(members)
            else:
                verbose_log(f"Account {account_id}: members is not a list for table display")
                member_count = 0
            
            mdfile.write(f"| {account_id} | {account_name} | {member_count} |\n")
        
        mdfile.write("\n")
        
        # Create detailed account sections with member tables
        mdfile.write("## Account Details\n\n")
        
        for account in sorted(accounts_with_members, key=lambda x: x.get('name', '')):
            account_id = account.get('id', '')
            account_name = account.get('name', '')
            members = account.get('members')
            
            # Validate members is a list
            if not isinstance(members, list):
                verbose_log(f"Account {account_id}: 'members' is not a list for markdown: {type(members)}")
                members = []
            
            mdfile.write(f"### {account_name}\n\n")
            mdfile.write(f"Account ID: `{account_id}`\n\n")
            
            if not members:
                mdfile.write("*No members found for this account*\n\n")
                continue
            
            mdfile.write("| Member Email | Status | Roles |\n")
            mdfile.write("|--------------|--------|-------|\n")
            
            # Define a safe sort key function
            def safe_email_key(m):
                if not isinstance(m, dict):
                    return ""
                user = m.get('user')
                if not isinstance(user, dict):
                    return ""
                return user.get('email', '')
            
            # Filter valid members and sort
            valid_members = [m for m in members if isinstance(m, dict)]
            if len(valid_members) != len(members):
                verbose_log(f"Account {account_id}: Filtered {len(members) - len(valid_members)} invalid members for markdown")
            
            sorted_members = sorted(valid_members, key=safe_email_key)
            
            for member in sorted_members:
                # Get email and status safely
                user_data = member.get('user')
                if not isinstance(user_data, dict):
                    verbose_log(f"Account {account_id}: Member has invalid user data for markdown: {type(user_data)}")
                    user_data = {}
                
                email = user_data.get('email', '')
                status = member.get('status', '')
                
                # Process roles safely
                roles = []
                member_roles = member.get('roles')
                if member_roles is None:
                    # Just log once per account, not for every member
                    if email and status:  # Only log for seemingly valid members
                        verbose_log(f"Account {account_id}: Member with email {email} has null roles")
                    member_roles = []
                elif not isinstance(member_roles, list):
                    verbose_log(f"Account {account_id}: Member has invalid roles type for markdown: {type(member_roles)}")
                    member_roles = []
                
                # Iterate over roles safely with empty list fallback when None
                for role in member_roles or []:
                    if isinstance(role, dict) and 'name' in role:
                        roles.append(role['name'])
                    else:
                        roles.append(str(role))
                
                mdfile.write(f"| {email} | {status} | {', '.join(roles)} |\n")
            
            mdfile.write("\n")
    
    print(f"Markdown export complete: {MARKDOWN_FILE}")

async def main():
    try:
        print("Starting Cloudflare accounts and members export")
        start_time = datetime.now()
        
        # Setup authentication
        if not setup_auth():
            debug_log("Authentication setup failed. Exiting.")
            sys.exit(1)
        
        # Show full headers if in super debug mode
        if DEBUG and SHOW_TOKEN:
            debug_log(f"FULL HEADERS: {HEADERS}")
        
        async with aiohttp.ClientSession() as session:
            # Test connectivity
            if not await test_connectivity(session):
                print("API connectivity test failed. Exiting.")
                sys.exit(1)
            
            # Get all accounts
            accounts = await get_accounts(session)
            
            if not accounts:
                print("No accounts found. Exiting.")
                sys.exit(1)
            
            # For each account, get its members (concurrently)
            accounts_with_members = await get_all_members(session, accounts)
            
            # Write the data to CSV and Markdown
            write_to_csv(accounts_with_members)
            write_to_markdown(accounts_with_members)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"Success! Export completed in {duration:.2f} seconds")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check required modules
    required_modules = ['aiohttp', 'asyncio']
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print("Error: Missing required modules.")
        print(f"Please install them using: pip install {' '.join(missing_modules)}")
        sys.exit(1)
    
    # Run the async main function
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
