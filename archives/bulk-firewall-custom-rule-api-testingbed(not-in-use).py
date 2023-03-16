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

# Make a request to the zones API endpoint to get the zone ids
response = requests.get(BASE_URL, headers=headers)

# Parse the response as JSON
data = response.json()

firewall_rule_id_data = []
# Iterate over the zone ids
for zone_ids in data["result"]:

    # Get the ID from the current item
    zone_id = zone_ids["id"]

    # Make a request to the firewall rules endpoint with the current ID to get the list of firewall rules
    page = 1
    while True:
       # if zone_id == "3f2c4daa43d5920f313654a873b31d06":
        second_api_url = BASE_URL + \
                f"/{zone_id}/firewall/rules?page={page}&per_page=1000"
        response = requests.get(second_api_url, headers=headers)
        firewall_id_page_data = response.json()
        # print(firewall_id_page_data)
        if len(data["result"]) == 0:
            break
        firewall_rule_id_data.extend(firewall_id_page_data["result"])
        page += 1
        firewall_rules_data = []
        # Iterate over the data from the current page of the firewall rules endpoint
        for firewall_rule_ids in firewall_rule_id_data["result"]:

            # Get the ID from the current item
            firewall_rule_id = firewall_rule_ids["id"]

            # Make a request to the firewall rules endpoint for the specific ID using the current IDs
            third_api_url = BASE_URL + \
                f"/{zone_id}/firewall/rules/{firewall_rule_id}"
            response = requests.get(third_api_url, headers=headers)
            firewall_rule_data = response.json()
            firewall_rule = firewall_rule_data.get("result")
            firewall_rule_transform = {}
            
            # Extract top-level prop from the firewall rule payload
            firewall_rule_transform["description"] = firewall_rule["description"]
            firewall_rule_transform["action"] = firewall_rule["action"]
            if firewall_rule_transform["action"] == "bypass":
                firewall_rule_transform["action"] = "skip"
            
            # Extract nested objects from the firewall rule payload
            filter = firewall_rule["filter"]
            firewall_rule_transform["expression"] = filter["expression"]
            firewall_rules_data.append(firewall_rule_transform)
            # print(firewall_rule_transform)
            
            
        # Create the object that will contain the "rules" array to be used in custom rules API
        rules_data = {}
        rules_data.update(firewall_rules_data)
        # print(rules_data)
        # Create the "rules" array to be used in the custom rules APi
        rules_data["rules"] = []
            
        # Add the firewall rule payloads from before into "rules" array
        rules_data["rules"].append(firewall_rules_data)
        # print(rules_data)

        # Get the current rules from the ruleset
        rulesets_url_current = BASE_URL + \
            f"/{zone_id}/rulesets/7b60160de9754d98b5dd1e9dea12944f"
        response = requests.get(rulesets_url_current, headers=headers)
        data = response.json()
        result_rulesets_url_current = data.get("result")
        rules_rulesets_url_current = result_rulesets_url_current.get("rules")
        #print(response.text)
            
        # Add the payload from the ruleset to the "rules" array
        rules_data["rules"].append(rules_rulesets_url_current)
        #  print(rules_data)
        # Add the final payload to the custom rules API for migration
        rulesets_url_current = BASE_URL + \
            f"/{zone_id}/rulesets/7b60160de9754d98b5dd1e9dea12944f"
        response = requests.put(rulesets_url_current, headers=headers, json=rules_data)
        print(response.text)

