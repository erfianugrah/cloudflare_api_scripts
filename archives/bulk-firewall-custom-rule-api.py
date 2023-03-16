import os
import requests

# Base endpoint for Cloudflare's API for zones
BASE_URL = "https://api.cloudflare.com/client/v4/zones"

# Define Auth headers
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

# Page params
params = {
    "page": 1,
    "per_page": 1000
}

# Set headers
headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}


# Make a request to the zones API endpoint to get the zone ids
response = requests.get(BASE_URL, headers=headers, params=params)

# Parse the response as JSON
data = response.json()

# Create the object that will contain the "rules" array to be used in custom rules API
rules_data = {}

# Create the "rules" array to be used in the custom rules API
rules_data["rules"] = []
    
# Iterate over the zone ids
for zone_ids in data["result"]:

    # Get the ID from the current item
    zone_id = zone_ids["id"]
    
    if zone_id == "3f2c4daa43d5920f313654a873b31d06":
        
        # Start pagination while loop
        page = 1
        while True:
            
            # Make a request to the firewall rules endpoint with the current ID to get the list of firewall rules
            firewall_rules_api = BASE_URL + \
                f"/{zone_id}/firewall/rules?page={page}&per_page=1000"
            response = requests.get(firewall_rules_api, headers=headers)
            data = response.json()
            
            # Create the object that will contain the firewall rules that's been transformed
            firewall_rule_transform = {}

            # Iterate over the data from the current page of the firewall rules endpoint
            for firewall_rule_ids in data["result"]:

                # Get the ID from the current item
                firewall_rule_id = firewall_rule_ids["id"]

                # Make a request to the firewall rules endpoint for the specific ID
                firewall_rules_id_api = BASE_URL + \
                    f"/{zone_id}/firewall/rules/{firewall_rule_id}"
                response = requests.get(firewall_rules_id_api, headers=headers)
                data = response.json()

                # Get the object from the result payload
                firewall_rule = data["result"]
                
                # Extract top-level prop from the firewall rule payload
                firewall_rule_transform["description"] = firewall_rule["description"]
                firewall_rule_transform["action"] = firewall_rule["action"]
                
                # Fix those bypass rules into skip, since bypass doesn't exist in custom rules
                if firewall_rule_transform["action"] == "bypass":
                    firewall_rule_transform["action"] = "skip"

                # Extract nested objects from the firewall rule payload
                filters = firewall_rule["filter"]
                firewall_rule_transform["expression"] = filters["expression"]
                firewall_rule_transform["enabled"] = filters["paused"]
                
                # Set rule to enabled: true
                if filters["paused"] == "false":
                    firewall_rule_transform["enabled"] = "true"
                else:
                    firewall_rule_transform["enabled"] = "false"
                    
                # Add the firewall rule objects from before into "rules" array in the ruleset
                rules_data["rules"].append(firewall_rule_transform)
                print(rules_data)
            # Check if there are more pages of results
            if not data["result"]:
                break

            # Move to the next page of results
            page += 1
            
        # Get list of rulesets from zone
        rulesets_api = BASE_URL + \
            f"/{zone_id}/rulesets"
        response = requests.get(rulesets_api, headers=headers)
        data = response.json()
        
        # Create object for the transformed rules in the rulesets
        rulesets_current_payload_transform = {}

        # Iterate over the data from
        for ruleset_ids in data["result"]:
            if ruleset_ids["phase"] == "http_request_firewall_custom":
                ruleset_id = ruleset_ids["id"]
            
                # Get the current rules from the ruleset
                rulesets_id_api = BASE_URL + \
                    f"/{zone_id}/rulesets/{ruleset_id}"
                response = requests.get(rulesets_id_api, headers=headers)

                # Add the payload from the ruleset to the "rules" array
                data = response.json()
                
                # If fresh migration, therefore custom rules is empty
                for rules in data:
                    if "rules" not in rules:
                        
                        # Add the final payload to the custom rules API for migration
                        rulesets_specific_id_api = BASE_URL + \
                            f"/{zone_id}/rulesets/{ruleset_id}"
                        response = requests.put(rulesets_specific_id_api, headers=headers, json=rules_data)
                    else:
                        rulesets_current_payload = data["result"]["rules"]
        
                        # Iterate list items in list to create new list and append to the current list with the transformed firewall rules
                        for rule in rulesets_current_payload:
                            rulesets_current_payload_transform["action"] = rule["action"]
                            rulesets_current_payload_transform["expression"] = rule["expression"]
                            rulesets_current_payload_transform["description"] = rule["description"]
                            rulesets_current_payload_transform["enabled"] = rule["enabled"]
                            # if rulesets_current_payload_transform["enabled"] == "true":
                            #     rulesets_current_payload_transform["enabled"] = "false"
                            
                            # Add current custom rules to the list
                            rules_data["rules"].append(rulesets_current_payload_transform)

                            # Add the final payload to the custom rules API for migration
                            rulesets_specific_id_api = BASE_URL + \
                                f"/{zone_id}/rulesets/{ruleset_id}"
                            response = requests.put(rulesets_specific_id_api, headers=headers, json=rules_data)
                            print(response.json)