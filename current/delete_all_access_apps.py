import os
import requests

# Base endpoint for Cloudflare's API
BASE_URL = "https://api.cloudflare.com/client/v4/accounts"

# Define Auth headers
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

# Set headers
headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}

def get_user_account_ids():
    # Prompt the user to enter zone IDs, separated by commas
    user_input = input("Enter the account IDs, separated by commas: ")
    # Split the input string into a list of zone IDs
    account_ids = [account_id.strip() for account_id in user_input.split(',')]
    return account_ids

def list_access_apps(account_id):
    access_apps_list = []
    page = 1
    while True:
        endpoint = f"{BASE_URL}/{account_id}/access/apps?page={page}&per_page=100"
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve Access applications. Status code: {response.status_code}")
            print(response.text)
            break
        data = response.json()
        access_apps = data["result"]
        access_apps_list.extend(access_apps)
        if not access_apps:
            break
        page += 1
    return access_apps_list

def delete_access_app(account_id, app_id):
    endpoint = f"{BASE_URL}/{account_id}/access/apps/{app_id}"
    response = requests.delete(endpoint, headers=headers)
    if response.status_code == 200:
        print(f"Deleted Access application {app_id} successfully.")
    else:
        print(f"Failed to delete Access application {app_id}. Status code: {response.status_code}")
        print(response.text)

# Get zone IDs from the user
account_ids = get_user_account_ids()

# Loop through the list of user-supplied zone IDs
for account_id in account_ids:
    # List all access applications for the zone
    access_apps = list_access_apps(account_id)
    print(f"Found {len(access_apps)} Access applications in account {account_id}. Deleting...")
    # Loop through the access applications and delete them
    for app in access_apps:
        delete_access_app(account_id, app["id"])
