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

def list_service_tokens(account_id):
    service_tokens_list = []
    page = 1
    while True:
        endpoint = f"{BASE_URL}/{account_id}/access/service_tokens?page={page}&per_page=100"
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve service tokens. Status code: {response.status_code}")
            print(response.text)
            break
        data = response.json()
        service_tokens = data["result"]
        service_tokens_list.extend(service_tokens)
        if not service_tokens:
            break
        page += 1
    return service_tokens_list 

def delete_service_token(account_id, token_id):
    endpoint = f"{BASE_URL}/{account_id}/access/service_tokens/{token_id}"
    response = requests.delete(endpoint, headers=headers)
    if response.status_code == 200:
        print(f"Deleted service token {token_id} successfully.")
    else:
        print(f"Failed to delete service token{token_id}. Status code: {response.status_code}")
        print(response.text)

# Get zone IDs from the user
account_ids = get_user_account_ids()

# Loop through the list of user-supplied zone IDs
for account_id in account_ids:
    # List all access applications for the zone
    service_tokens = list_service_tokens(account_id)
    print(f"Found {len(service_tokens)} service token in account {account_id}. Deleting...")
    # Loop through the access applications and delete them
    for token in service_tokens:
        delete_service_token(account_id, token["id"])
