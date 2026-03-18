# Cloudflare API Scripts

Three async CLI tools that consolidate 30+ individual scripts into a shared library with consistent auth, rate limiting, pagination, and UX.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Authentication

All three tools accept credentials via environment variables or CLI flags. Bearer token is recommended; API key + email is also supported.

```bash
# Option A: Bearer token (recommended)
export CLOUDFLARE_API_TOKEN="your-token"

# Option B: API key + email
export CLOUDFLARE_API_KEY="your-key"
export CLOUDFLARE_EMAIL="your-email"

# Optional (used by Zero Trust commands)
export CLOUDFLARE_ACCOUNT_ID="your-account-id"
```

CLI flags (`--api-token`, `--api-key`, `--email`) override env vars when provided.

## Tools

### `cf_dns.py` -- DNS & Zone Management

| Command | Description | Replaces |
|---------|-------------|----------|
| `zones` | List all zones | `list_all_zone_ids_in_a_list.py` |
| `records` | List DNS records (all or `--proxied-only`) | `list_all_proxied_dns_records.py` |
| `delete-records` | Delete all DNS records in specified zones | `delete_all_dns_records.py` |
| `hostnames` | List custom hostnames across zones | `list_all_custom_hostnames.py`, `async_list_all_custom_hostnames.py` |
| `certs` | List non-active certificate packs | `list_non_active_certs.py` |
| `pending` | Fetch pending zones with TXT/DCV verification info | `fetch_all_pending_zones_txt.py` |
| `dmarc` | Enable DMARC reporting for zones | `enable_dmarc_all_zones.py` |
| `spectrum` | List or delete Spectrum apps | `delete_all_spectrum_apps.py` |

```bash
python cf_dns.py zones
python cf_dns.py zones --json
python cf_dns.py records --proxied-only
python cf_dns.py records --zone-ids abc123,def456 --output records.json
python cf_dns.py delete-records --zone-ids abc123
python cf_dns.py hostnames
python cf_dns.py certs
python cf_dns.py pending --account ACCT_ID --output pending.csv
python cf_dns.py dmarc
python cf_dns.py spectrum --zone-ids abc123
python cf_dns.py spectrum --zone-ids abc123 --delete
```

### `cf_security.py` -- Security Rules & Rulesets

| Command | Description | Replaces |
|---------|-------------|----------|
| `delete-fw-rules` | Delete firewall rules (all or by `--description`) | `delete_all_firewall_rules.py`, `delete_all_firewall_rules_with_the_same_description.py` |
| `find-filters` | Search filters by expression across zones | `delete_all_filters_by_expression.py` |
| `init-rulesets` | Initialize empty custom rulesets for zones | `intiate_custom_rulesets_for_zones.py` |
| `clear-ruleset` | Clear all rules in a ruleset phase | `delete_all_rules_in_a_ruleset_all_zones.py`, `delete_all_rules_in_a_ruleset_one_zone.py` |
| `purge-versions` | Delete old (non-latest) ruleset versions | `delete_rules_in_ruleset_via_version_id.py` |
| `revert` | Revert a ruleset to a previous or specific version | `revert_to_previous_version_for_custom_ruleset.py`, `revert_to_previous_version_for_managed_ruleset.py`, `revert_to_specific_version_for_custom_ruleset.py` |
| `migrate` | Migrate legacy firewall rules to WAF custom rules | `migrate_firewall_rules_to_custom_rules.py` |
| `tf-import` | Import rulesets into Terraform state | `bulk_import_tf_rulesets.py` |

```bash
python cf_security.py find-filters --expression "ip.src"
python cf_security.py delete-fw-rules --description "Block bots"
python cf_security.py init-rulesets
python cf_security.py clear-ruleset --phase http_request_firewall_custom
python cf_security.py clear-ruleset --phase http_request_firewall_custom --zone-ids abc123
python cf_security.py purge-versions --phase http_request_firewall_custom
python cf_security.py revert --zone-id abc123 --phase http_request_firewall_custom
python cf_security.py revert --zone-id abc123 --ruleset-id def456 --version 3
python cf_security.py migrate --dry-run
python cf_security.py tf-import --phase http_request_firewall_custom
```

### `cf_access.py` -- Access & Zero Trust

| Command | Description | Replaces |
|---------|-------------|----------|
| `apps` | List or delete Access applications | `delete_all_access_apps.py` |
| `idps` | List or delete Identity Providers | `delete_all_idps.py` |
| `tokens` | List or delete service tokens | `delete_all_service_tokens.py` |
| `members` | List account members across all accounts | `list_members.py` |
| `users` | Audit ZT users, delete inactive, clear seats | `async_get_all_active_sessions.py`, `async_remove_inactive_users.py`, `async_zt_seat_manager.py` |

```bash
python cf_access.py apps --account-ids ACCT_ID
python cf_access.py apps --account-ids ACCT_ID --delete
python cf_access.py idps --account-ids ACCT_ID
python cf_access.py tokens --account-ids ACCT_ID
python cf_access.py members
python cf_access.py members --output members.csv
python cf_access.py users --account-id ACCT_ID
python cf_access.py users --account-id ACCT_ID --output users.json
python cf_access.py users --account-id ACCT_ID --delete-inactive --dry-run
python cf_access.py users --account-id ACCT_ID --remove-seats --inactive-only
```

## Shared Library (`cf_lib/`)

All three tools use `cf_lib/` which provides:

- **`CloudflareClient`** -- Async API client (`aiohttp`) with:
  - Unified auth (Bearer token or API key + email, from env or args)
  - Adaptive rate limiting using RFC 9110 `Ratelimit` / `Ratelimit-Policy` response headers
  - Automatic backpressure when budget drops below 10% of quota
  - 429 retry with `Retry-After` as fallback
  - Concurrency control via semaphore (default: 10 parallel requests)
  - Generic pagination for any Cloudflare list endpoint
  - Convenience helpers: `get_all_zones()`, `get_all_accounts()`

- **`output`** -- Shared output helpers:
  - `print_table()` -- aligned ASCII tables
  - `write_json()` / `write_csv()` -- file export
  - `progress()` -- inline progress bar
  - `confirm()` -- y/N confirmation prompt
  - `add_auth_args()` -- adds `--api-token`, `--api-key`, `--email` to any argparse parser

## Standalone Scripts (kept as-is)

| Script | Purpose |
|--------|---------|
| `cf_ip_checker.py` | Check/list/count/compare Cloudflare IP ranges (no auth needed) |
| `async_dns_resolution.py` | Bulk DNS resolution from CSV/TXT (not a CF API script) |
| `nmap.py` | Network scanner wrapper (not a CF API script) |
| `async_list_r2_objects_per_bucket.py` | List R2 bucket objects |

## Firewall Rule Migration Workflow

```bash
# 1. Initialize custom rulesets (prerequisite)
python cf_security.py init-rulesets

# 2. Preview migration
python cf_security.py migrate --dry-run

# 3. Run migration
python cf_security.py migrate

# 4. If you need to roll back
python cf_security.py revert --zone-id ZONE_ID --phase http_request_firewall_custom

# 5. Once verified, remove old firewall rules
python cf_security.py delete-fw-rules
```

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.
