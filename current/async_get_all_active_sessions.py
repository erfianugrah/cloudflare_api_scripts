import asyncio
import aiohttp
import os
from typing import List, Dict
import csv
from tabulate import tabulate
from datetime import datetime, timezone
import matplotlib.pyplot as plt

# Define base URL
BASE_URL = 'https://api.cloudflare.com/client/v4/accounts'

# Define Auth headers
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

# Set headers
headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}

async def get_users(session: aiohttp.ClientSession, account_id: str) -> List[Dict]:
    url = f'{BASE_URL}/{account_id}/access/users?per_page=1000'
    async with session.get(url, headers=headers) as response:
        data = await response.json()
        return data['result']

async def get_active_sessions(session: aiohttp.ClientSession, account_id: str, user_id: str) -> List[Dict]:
    url = f'{BASE_URL}/{account_id}/access/users/{user_id}/active_sessions'
    async with session.get(url, headers=headers) as response:
        data = await response.json()
        return data['result']

def format_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

async def process_user(session: aiohttp.ClientSession, account_id: str, user: Dict) -> Dict:
    user_id = user['id']
    email = user['email']
    name = user['name']
    
    active_sessions = await get_active_sessions(session, account_id, user_id)
    
    if active_sessions:
        session = active_sessions[0]
        expiration = format_timestamp(session['expiration'])
        apps = session['metadata']['apps']
        app_info = next(iter(apps.values()))  # Get the first (and typically only) app
        hostname = app_info.get('hostname', 'N/A')
        app_name = app_info.get('name', 'N/A')
        app_type = app_info.get('type', 'N/A')
        app_uid = app_info.get('uid', 'N/A')
        last_issued = format_timestamp(session['metadata']['iat'])
    else:
        expiration = hostname = app_name = app_type = app_uid = last_issued = 'N/A'
    
    return {
        'Name': name,
        'Email': email,
        'Active Sessions': len(active_sessions),
        'Session Expiration': expiration,
        'App Hostname': hostname,
        'App Name': app_name,
        'App Type': app_type,
        'App UID': app_uid,
        'Last Issued': last_issued
    }

def get_account_id() -> str:
    account_id = os.environ.get('CLOUDFLARE_ACCOUNT_ID')
    if account_id:
        print(f"Using Cloudflare Account ID from environment: {account_id}")
    else:
        account_id = input("Please enter your Cloudflare account ID: ").strip()
    return account_id

def create_chart(results: List[Dict], output_filename: str):
    names = [result['Name'] for result in results]
    active_sessions = [result['Active Sessions'] for result in results]

    plt.figure(figsize=(12, 6))
    plt.bar(names, active_sessions)
    plt.title('Active Sessions per User')
    plt.xlabel('User Name')
    plt.ylabel('Number of Active Sessions')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_filename)
    print(f"\nChart saved as {output_filename}")

async def main():
    account_id = get_account_id()

    async with aiohttp.ClientSession() as session:
        users = await get_users(session, account_id)
        
        tasks = [process_user(session, account_id, user) for user in users]
        results = await asyncio.gather(*tasks)

    # ASCII Table Output
    print(tabulate(results, headers='keys', tablefmt='grid'))

    # CSV Output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f'cloudflare_users_{timestamp}.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\nCSV file '{csv_filename}' has been created.")

    # Chart Output
    chart_filename = f'cloudflare_users_chart_{timestamp}.png'
    create_chart(results, chart_filename)

if __name__ == "__main__":
    asyncio.run(main())
