import asyncio
import aiohttp
import os
from typing import List, Dict
import csv
import json
from datetime import datetime, timezone
import matplotlib.pyplot as plt

BASE_URL = 'https://api.cloudflare.com/client/v4/accounts'

headers = {
    "X-Auth-Key": os.environ.get('CLOUDFLARE_API_KEY'),
    "X-Auth-Email": os.environ.get('CLOUDFLARE_EMAIL')
}

async def fetch_data(session: aiohttp.ClientSession, url: str) -> Dict:
    async with session.get(url, headers=headers) as response:
        return await response.json()

async def get_users(session: aiohttp.ClientSession, account_id: str) -> List[Dict]:
    data = await fetch_data(session, f'{BASE_URL}/{account_id}/access/users?per_page=1000')
    return data['result']

async def get_active_sessions(session: aiohttp.ClientSession, account_id: str, user_id: str) -> List[Dict]:
    data = await fetch_data(session, f'{BASE_URL}/{account_id}/access/users/{user_id}/active_sessions')
    return data['result']

def format_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

async def process_user(session: aiohttp.ClientSession, account_id: str, user: Dict) -> Dict:
    active_sessions = await get_active_sessions(session, account_id, user['id'])
    
    user_data = {
        'Name': user['name'],
        'Email': user['email'],
        'Active Sessions': len(active_sessions),
        'Apps': []
    }
    
    if active_sessions:
        session = active_sessions[0]
        user_data['Session Expiration'] = format_timestamp(session['expiration'])
        user_data['Last Issued'] = format_timestamp(session['metadata']['iat'])
        
        for app_data in session['metadata']['apps'].values():
            user_data['Apps'].append({
                'Name': app_data.get('name', 'N/A'),
                'Hostname': app_data.get('hostname', 'N/A'),
                'Type': app_data.get('type', 'N/A'),
                'UID': app_data.get('uid', 'N/A')
            })
    else:
        user_data['Session Expiration'] = 'N/A'
        user_data['Last Issued'] = 'N/A'
    
    return user_data

def create_chart(results: List[Dict], output_filename: str):
    names = [result['Name'] for result in results]
    active_sessions = [result['Active Sessions'] for result in results]
    app_counts = [len(result['Apps']) for result in results]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    ax1.bar(names, active_sessions)
    ax1.set_title('Active Sessions per User')
    ax1.set_xlabel('User Name')
    ax1.set_ylabel('Number of Active Sessions')
    ax1.tick_params(axis='x', rotation=45)

    ax2.bar(names, app_counts)
    ax2.set_title('Active Apps per User')
    ax2.set_xlabel('User Name')
    ax2.set_ylabel('Number of Active Apps')
    ax2.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig(output_filename)
    print(f"\nChart saved as {output_filename}")

def format_nested_dict(d, indent=0):
    result = []
    for key, value in d.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            result.append(f"{'  ' * indent}{key}:")
            for item in value:
                result.append(format_nested_dict(item, indent + 1))
        elif isinstance(value, dict):
            result.append(f"{'  ' * indent}{key}:")
            result.append(format_nested_dict(value, indent + 1))
        else:
            result.append(f"{'  ' * indent}{key}: {value}")
    return '\n'.join(result)

def generate_summary(results: List[Dict]) -> Dict:
    total_users = len(results)
    total_active_sessions = sum(user['Active Sessions'] for user in results)
    total_apps = sum(len(user['Apps']) for user in results)

    summary = {
        'Total Users': total_users,
        'Total Active Sessions': total_active_sessions,
        'Total Apps': total_apps,
        'Users': [
            {
                'Name': user['Name'],
                'Email': user['Email'],
                'Active Sessions': user['Active Sessions'],
                'Apps': len(user['Apps'])
            } for user in results
        ]
    }
    return summary

def print_summary(summary: Dict):
    print("\nAccount Summary:")
    print("-" * 50)
    print(f"Total Users: {summary['Total Users']}")
    print(f"Total Active Sessions: {summary['Total Active Sessions']}")
    print(f"Total Apps: {summary['Total Apps']}")
    print("\nPer User Summary:")
    print("-" * 50)
    for user in summary['Users']:
        print(f"{user['Name']} ({user['Email']}):")
        print(f"  Active Sessions: {user['Active Sessions']}")
        print(f"  Apps: {user['Apps']}")
    print("-" * 50)

async def main():
    account_id = os.environ.get('CLOUDFLARE_ACCOUNT_ID') or input("Enter your Cloudflare account ID: ").strip()

    async with aiohttp.ClientSession() as session:
        users = await get_users(session, account_id)
        results = await asyncio.gather(*[process_user(session, account_id, user) for user in users])

    # Generate and print summary
    summary = generate_summary(results)
    print_summary(summary)

    # Detailed console output
    print("\nDetailed User Information:")
    for user in results:
        print(format_nested_dict(user))
        print('-' * 50)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON Output
    json_filename = f'cloudflare_users_{timestamp}.json'
    with open(json_filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(results, jsonfile, indent=2)
    print(f"\nJSON file '{json_filename}' has been created.")

    # Detailed CSV Output (flattened structure)
    csv_filename = f'cloudflare_users_detailed_{timestamp}.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Name', 'Email', 'Active Sessions', 'Session Expiration', 'Last Issued', 
                      'App Name', 'App Hostname', 'App Type', 'App UID']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for user in results:
            base_row = {k: v for k, v in user.items() if k != 'Apps'}
            if user['Apps']:
                for app in user['Apps']:
                    row = base_row.copy()
                    row.update({f'App {k}': v for k, v in app.items()})
                    writer.writerow(row)
            else:
                writer.writerow(base_row)
    print(f"\nDetailed CSV file '{csv_filename}' has been created.")

    # Summary CSV Output
    summary_csv_filename = f'cloudflare_users_summary_{timestamp}.csv'
    with open(summary_csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Total Users', 'Total Active Sessions', 'Total Apps'])
        writer.writerow([summary['Total Users'], summary['Total Active Sessions'], summary['Total Apps']])
        writer.writerow([])  # Empty row for separation
        writer.writerow(['Name', 'Email', 'Active Sessions', 'Apps'])
        for user in summary['Users']:
            writer.writerow([user['Name'], user['Email'], user['Active Sessions'], user['Apps']])
    print(f"\nSummary CSV file '{summary_csv_filename}' has been created.")

    chart_filename = f'cloudflare_users_chart_{timestamp}.png'
    create_chart(results, chart_filename)

if __name__ == "__main__":
    asyncio.run(main())
