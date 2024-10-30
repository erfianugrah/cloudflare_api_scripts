import asyncio
import aiohttp
import os
from typing import List, Dict
from datetime import datetime, timezone

BASE_URL = 'https://api.cloudflare.com/client/v4/accounts'

headers = {
    "X-Auth-Key": os.environ.get('CLOUDFLARE_API_KEY'),
    "X-Auth-Email": os.environ.get('CLOUDFLARE_EMAIL')
}

async def fetch_data(session: aiohttp.ClientSession, url: str) -> Dict:
    async with session.get(url, headers=headers) as response:
        return await response.json()

async def delete_user(session: aiohttp.ClientSession, account_id: str, user_id: str, email: str) -> bool:
    url = f'{BASE_URL}/{account_id}/access/users/{user_id}'
    try:
        async with session.delete(url, headers=headers) as response:
            result = await response.json()
            success = result.get('success', False)
            if not success:
                print(f"Failed to remove {email}: {result.get('errors', ['Unknown error'])}")
            return success
    except Exception as e:
        print(f"Error removing {email}: {str(e)}")
        return False

async def get_users(session: aiohttp.ClientSession, account_id: str) -> List[Dict]:
    data = await fetch_data(session, f'{BASE_URL}/{account_id}/access/users?per_page=1000')
    return data['result']

async def check_user_sessions(session: aiohttp.ClientSession, account_id: str, user: Dict) -> bool:
    try:
        data = await fetch_data(session, f'{BASE_URL}/{account_id}/access/users/{user["id"]}/active_sessions')
        return len(data['result']) > 0
    except Exception as e:
        print(f"Error checking sessions for {user['email']}: {str(e)}")
        return True  # Assume active if we can't check to be safe

async def process_users(session: aiohttp.ClientSession, account_id: str):
    users = await get_users(session, account_id)
    print(f"\nFound {len(users)} total users")
    print("Checking active sessions...")

    # First, check all users in parallel
    check_tasks = [check_user_sessions(session, account_id, user) for user in users]
    session_results = await asyncio.gather(*check_tasks)
    
    # Identify inactive users
    inactive_users = [user for user, has_sessions in zip(users, session_results) if not has_sessions]
    active_count = len(users) - len(inactive_users)
    
    print(f"\nFound {active_count} active users")
    print(f"Found {len(inactive_users)} inactive users")

    if inactive_users:
        # Confirm before deletion
        confirm = input(f"\nReady to remove {len(inactive_users)} inactive users. Continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Operation cancelled.")
            return
        
        print("\nRemoving inactive users...")
        # Remove inactive users in parallel
        delete_tasks = [delete_user(session, account_id, user['id'], user['email']) 
                       for user in inactive_users]
        delete_results = await asyncio.gather(*delete_tasks)
        
        # Count successful deletions
        removed_count = sum(1 for result in delete_results if result)
        print(f"\nSuccessfully removed {removed_count} users")
        if removed_count != len(inactive_users):
            print(f"Failed to remove {len(inactive_users) - removed_count} users")
    else:
        print("\nNo inactive users found.")

    print(f"\nActive users remaining: {active_count}")

async def main():
    account_id = os.environ.get('CLOUDFLARE_ACCOUNT_ID') or input("Enter your Cloudflare account ID: ").strip()
    
    async with aiohttp.ClientSession() as session:
        await process_users(session, account_id)

if __name__ == "__main__":
    asyncio.run(main())
