# Migrate Firewall Rules to Custom Rules

## Remember to install requirements 
```python3 -m pip install -r requirements.txt```

> (you only need `os` and `requests` for these ones currently)

## API Keys/Tokens
The zone IDs used will depend on the scope of your API key, if you're a member of many orgs, then it'll run for any zones that are compatible. Also, the scripts assumes you have these keys set as environment variables in your shell of choice.

If you want to run scoped tokens instead, feel free to fork the repo or (if you're a collaborator) `git checkout -b <new_branch>` and change out the header variables to account for scoped tokens.

So instead of:

```
auth_email = os.environ.get('CLOUDFLARE_EMAIL')
auth_key = os.environ.get('CLOUDFLARE_API_KEY')

headers = {
    "X-Auth-Key": auth_key,
    "X-Auth-Email": auth_email
}
```

Use

```
token = os.environ.get("CLOUDFLARE_API_TOKEN")

headers = {
    "Authorization: f"Bearer {token}
}
```

Of course, use the actual names you've set them as.

These should be the necessary ones:
- `Account - Firewall Access - Edit`
- `Account - Account Settings - Edit`
- `Zone - Zone Settings - Read`
- `Zone - Firewall Service - Edit`
- `Zone - Zone WAF - Edit`

## If no Custom Rules have been made before, run:
```python3 initiate_custom_rulesets_for_zones.py``` 

This ensures that when you migrate it has something to add all the rules and you won't error out.

## Run the script:
```python3 migrate_firewall_rules_to_custom_rules.py```

## If you change your mind, run:
```python3 delete_all_rules_in_a_ruleset.py```

## Once tested and ready to move off Firewall Rules entirely, run:
```python3 delete_all_firewall_rules.py```
