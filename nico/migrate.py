import httpx
import json
import os


def create_firewall_rules_list(client, zone_id):
    page_num = 1
    firewall_rules_list = []
    while True:
        api_path = zone_id + '/firewall/rules?page=' + \
            str(page_num) + '&per_page=50'
        r = client.get(
            api_path,
            timeout=30
        )

        if r.status_code != 200:
            print("Status Code: {}".format(r.status_code))
            print("MESSAGE: {}".format(r.text))
            return (False)

        firewall_rules = json.loads(r.text)['result']
        if firewall_rules:
            firewall_rules_list.extend(firewall_rules)
            page_num += 1
        else:
            break

    return (firewall_rules_list)


def get_ruleset_id(client, zone_id, phase):
    api_path = zone_id + '/rulesets'
    r = client.get(
        api_path,
        timeout=30
    )

    if r.status_code != 200:
        print("Status Code: {}".format(r.status_code))
        print("MESSAGE: {}".format(r.text))
        return (False)

    rulesets = json.loads(r.text)['result']
    for ruleset in rulesets:
        if ruleset["phase"] == phase:
            return (ruleset["id"])

    return (False)


def update_zone_ruleset(client, zone_id, ruleset_id, custom_rules_list):
    api_path = zone_id + '/rulesets/' + ruleset_id
    payload = {}
    payload["rules"] = custom_rules_list
    print(json.dumps(payload))
    r = client.put(
        api_path,
        data=json.dumps(payload),
        timeout=30
    )

    if r.status_code != 200:
        print("Status Code: {}".format(r.status_code))
        print("MESSAGE: {}".format(r.text))
        return (False)

    return (True)


def migrate_fw_rules(client, zone_ids):
    # This function will read all firewall rules per zone and create them as Custom Rules
    for zone_id in zone_ids:
        print("Zone ID: {}".format(zone_id))

        ruleset_id = get_ruleset_id(client=client, zone_id=zone_id,
                                    phase="http_request_firewall_custom")
        # Existing Custom Rules are NOT preserved, and will be deleted.
        # TODO: create a function to get_existing_custom_rules_list() and extend custom_rules_list
        custom_rules_list = []

        firewall_rules_list = create_firewall_rules_list(
            client=client, zone_id=zone_id)
        for firewall_rule in firewall_rules_list:
            print(firewall_rule)
            custom_rule = {}
            custom_rule["enabled"] = not firewall_rule["paused"]
            custom_rule["description"] = firewall_rule["description"]
            custom_rule["expression"] = firewall_rule["filter"]["expression"]
            custom_rule["action"] = firewall_rule["action"]
            if custom_rule["action"] == "bypass":
                custom_rule["action"] = "skip"
                custom_rule["action_parameters"] = {}
                custom_rule["action_parameters"]["products"] = firewall_rule["products"]
            custom_rules_list.append(custom_rule)

        # Append custom_rules_list to the Custom Rules ruleset for this zone
        update_zone_ruleset(
            client=client, zone_id=zone_id, ruleset_id=ruleset_id, custom_rules_list=custom_rules_list)


if __name__ == "__main__":
    # Initialize variables for use with Cloudflare API
    auth_email = os.environ.get('CLOUDFLARE_EMAIL')
    auth_key = os.environ.get('CLOUDFLARE_API_KEY')
    auth_headers = {
        "X-Auth-Key": auth_key,
        "X-Auth-Email": auth_email
    }
    client = httpx.Client(headers=auth_headers,
                          base_url="https://api.cloudflare.com/client/v4/zones/")

    # Get a list of the zone_ids to run the migration on
    # Read from file or take as command line argument
    zone_ids = ["bb86f947e760f9fa051bc4c1690ae4d6"]

    # Call main function
    migrate_fw_rules(client=client, zone_ids=zone_ids)
