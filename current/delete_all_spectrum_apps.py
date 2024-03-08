import os
import requests

# Base endpoint for Cloudflare's API
BASE_URL = "https://api.cloudflare.com/client/v4/zones"

# Define Auth headers
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

# Set headers
headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}

def get_user_zone_ids():
    # Prompt the user to enter zone IDs, separated by commas
    user_input = input("Enter the zone IDs, separated by commas: ")
    # Split the input string into a list of zone IDs
    zone_ids = [zone_id.strip() for zone_id in user_input.split(',')]
    return zone_ids

def list_spectrum_apps(zone_id):
    spectrum_apps_list = []
    page = 1
    while True:
        endpoint = f"{BASE_URL}/{zone_id}/spectrum/apps?page={page}&per_page=100"
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve Spectrum applications. Status code: {response.status_code}")
            print(response.text)
            break
        data = response.json()
        spectrum_apps = data["result"]
        spectrum_apps_list.extend(spectrum_apps)
        if not spectrum_apps:
            break
        page += 1
    return spectrum_apps_list

def delete_spectrum_app(zone_id, app_id):
    endpoint = f"{BASE_URL}/{zone_id}/spectrum/apps/{app_id}"
    response = requests.delete(endpoint, headers=headers)
    if response.status_code == 200:
        print(f"Deleted Spectrum application {app_id} successfully.")
    else:
        print(f"Failed to delete Spectrum application {app_id}. Status code: {response.status_code}")
        print(response.text)

# Get zone IDs from the user
zone_ids = get_user_zone_ids()

# Loop through the list of user-supplied zone IDs
for zone_id in zone_ids:
    # List all Spectrum applications for the zone
    spectrum_apps = list_spectrum_apps(zone_id)
    print(f"Found {len(spectrum_apps)} Spectrum applications in zone {zone_id}. Deleting...")
    # Loop through the Spectrum applications and delete them
    for app in spectrum_apps:
        delete_spectrum_app(zone_id, app["id"])
