import os
import requests

# Base endpoint for Cloudflare's API for zones
BASE_URL = "https://api.cloudflare.com/client/v4/zones"

# Define Auth headers
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

# Set headers
headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}

zone_id = input("Enter zone_id: ")

def list_and_print_all_phases(BASE_URL, headers, zone_id):
    
    ruleset_names = []
    # Get list of rulesets from zone
    rulesets_api = BASE_URL + f"/{zone_id}/rulesets"
    response = requests.get(rulesets_api, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data from List Rulesets API. Status code: {response.status_code}")

    data = response.json()

    for phases in data["result"]:
        if "source" in phases:
            if phases["source"] == "firewall_managed" and not phases["name"] == "zone":
                phase = {}
                phase["phase"] = phases["phase"]
                phase["name"] = phases["name"]
                phase["id"] = phases["id"]
                ruleset_names.append(phase)
                
    print(ruleset_names)
    return phase

list_and_print_all_phases(BASE_URL, headers, zone_id)

ruleset_id = input("Input ruleset ID: ")

# def get_custom_ruleset_id(BASE_URL, headers, zone_id, phase):

#     # Get list of rulesets from zone
#     rulesets_api = BASE_URL + f"/{zone_id}/rulesets"
#     response = requests.get(rulesets_api, headers=headers)
    
#     if response.status_code != 200:
#         raise Exception(f"Failed to retrieve data from List Rulesets API. Status code: {response.status_code}")

#     data = response.json()
    
#     for ruleset_ids in data["result"]:
#         if phase in ruleset_ids["phase"]:
#             ruleset_id = ruleset_ids["id"]
    
#     return ruleset_id

def get_previous_version_id(BASE_URL, headers, zone_id, ruleset_id):
    
    # # Get Ruleset ID
    # ruleset_id = get_custom_ruleset_id(BASE_URL, headers, zone_id, phase)
    
    # Get versions objects
    rulesets_versions_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}/versions"
    response = requests.get(rulesets_versions_api, headers=headers)
    data = response.json()

    version_id = data["result"][1]["version"]
    
    return version_id

def get_previous_version_id_data(BASE_URL, headers, zone_id, ruleset_id):
    
    # # Get Ruleset ID
    # ruleset_id = get_custom_ruleset_id(BASE_URL, headers, zone_id, phase)
    
    # Get previous version ID
    version_id = get_previous_version_id(BASE_URL, headers, zone_id, ruleset_id)
    
    # Instantiate new list
    managed_ruleset = []

    # Get the current rules from the custom ruleset
    rulesets_id_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}/versions/{version_id}"
    response = requests.get(rulesets_id_api, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data from List Rulesets API with specific ruleset id. Status code: {response.status_code}")

    data = response.json()
    
    rules = data["result"]["rules"]
   
    # Add data to new list
    managed_ruleset.append(rules)
                    
    return managed_ruleset

def revert(BASE_URL, headers, zone_id, ruleset_id):
    # Get Ruleset ID
    # ruleset_id = get_custom_ruleset_id(BASE_URL, headers, zone_id, phase)
    
    # Get previous version
    previous_managed_rules = get_previous_version_id_data(BASE_URL, headers, zone_id, ruleset_id)
    
    # Instantiate object
    payload = {}
    
    # Set up payload
    payload["rules"] = previous_managed_rules
    
    # Get the current rules from the custom ruleset
    rulesets_id_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}"
    response = requests.put(rulesets_id_api, headers=headers, json=payload)
    print(response)
    
    if response.status_code != 200:
        raise Exception(f"Failed to add data to Rulesets API with specific ruleset id. Status code: {response.status_code}")
    
    return response

revert(BASE_URL, headers, zone_id, ruleset_id)