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
            if phases["source"] == "firewall_custom" and phases["name"] == "Default":
                phase = {}
                phase["phase"] = phases["phase"]
                phase["name"] = phases["name"]
                phase["id"] = phases["id"]
                ruleset_names.append(phase)
    print(ruleset_names)
    return ruleset_names

list_and_print_all_phases(BASE_URL, headers, zone_id)

ruleset_id = input("Input ruleset_id: ")

def get_all_version_ids_data(BASE_URL, headers, zone_id, ruleset_id):
    
    current_version_ids = []
    
    # Get versions objects
    rulesets_version_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}/versions"
    response = requests.get(rulesets_version_api, headers=headers)
    data = response.json()

    # Iterate over the version ids and skipping the latest one, which is usually in the first object, since you can't delete the latest version as it is the same as the pinned version at this point         
    for ruleset_version_ids in data["result"]:
        for version_id in ruleset_version_ids["version"]:
            version_id = ruleset_version_ids["version"]
            
            rulesets_id_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}/versions/{version_id}"
            response = requests.get(rulesets_id_api, headers=headers)
            data = response.json()
            
            if "rules" in data["result"]:
                rules = {}
                rules["rules"] = data["result"]["rules"]
                rules["version"] = data["result"]["version"]
                current_version_ids.append(rules)
    
    print(current_version_ids)
    return current_version_ids
    
get_all_version_ids_data(BASE_URL, headers, zone_id, ruleset_id)

version_id = input("Enter version_id: ")

def get_custom_ruleset_data(BASE_URL, headers, zone_id, ruleset_id, version_id):
    
    # Instantiate new list
    custom_ruleset = []

    # Get the current rules from the custom ruleset
    rulesets_specific_version_id_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}/versions/{version_id}"
    response = requests.get(rulesets_specific_version_id_api, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data from List Rulesets API with specific ruleset id. Status code: {response.status_code}")

    data = response.json()
    
    if "rules" in data["result"]:
        rules = data["result"]["rules"]
        
        # Transform data
        for rule in rules:
            current_custom_ruleset = {}
            current_custom_ruleset["action"] = rule["action"]
            current_custom_ruleset["expression"] = rule["expression"]
            current_custom_ruleset["description"] = rule["description"]
            current_custom_ruleset["enabled"] = rule["enabled"]
            if "logging" in rule:
                current_custom_ruleset["logging"] = rule["logging"]
            if "action_parameters" in rule:
                current_custom_ruleset["action_parameters"] = rule["action_parameters"]
                
            # Add data to new list
            custom_ruleset.append(current_custom_ruleset)
                    
    return custom_ruleset

def revert(BASE_URL, headers, zone_id, ruleset_id, version_id):
    
    # Get previous version
    specific_version = get_custom_ruleset_data(BASE_URL, headers, zone_id, ruleset_id, version_id)
    
    # Instantiate object
    payload = {}
    
    # Set up payload
    payload["rules"] = specific_version
    
    # Get the current rules from the custom ruleset
    rulesets_id_api = BASE_URL + f"/{zone_id}/rulesets/{ruleset_id}"
    response = requests.put(rulesets_id_api, headers=headers, json=payload)
    
    if response.status_code != 200:
        raise Exception(f"Failed to add data to Rulesets API with specific ruleset id. Status code: {response.status_code}")
    
    print(response)
    return response

revert(BASE_URL, headers, zone_id, ruleset_id, version_id)