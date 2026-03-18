#!/usr/bin/env python3
"""
Cloudflare Security Rules & Rulesets CLI

Consolidates firewall rules, filters, and ruleset management into a single
async tool.

Replaces:
  - delete_all_firewall_rules.py
  - delete_all_firewall_rules_with_the_same_description.py
  - delete_all_filters_by_expression.py
  - intiate_custom_rulesets_for_zones.py
  - delete_all_rules_in_a_ruleset_all_zones.py
  - delete_all_rules_in_a_ruleset_one_zone.py
  - delete_rules_in_ruleset_via_version_id.py
  - revert_to_previous_version_for_custom_ruleset.py
  - revert_to_previous_version_for_managed_ruleset.py
  - revert_to_specific_version_for_custom_ruleset.py
  - migrate_firewall_rules_to_custom_rules.py
  - bulk_import_tf_rulesets.py

Usage:
    python cf_security.py delete-fw-rules
    python cf_security.py delete-fw-rules --description "Block bots"
    python cf_security.py find-filters --expression "ip.src"
    python cf_security.py init-rulesets
    python cf_security.py clear-ruleset --phase http_request_firewall_custom
    python cf_security.py clear-ruleset --phase http_request_firewall_custom --zone-ids abc
    python cf_security.py purge-versions --phase http_request_firewall_custom
    python cf_security.py revert --zone-id abc --phase http_request_firewall_custom
    python cf_security.py revert --zone-id abc --ruleset-id def --version 3
    python cf_security.py migrate
    python cf_security.py tf-import --phase http_request_firewall_custom
"""

import argparse
import asyncio
import subprocess
import sys

from cf_lib import CloudflareClient, AuthConfig
from cf_lib.output import (
    add_auth_args,
    confirm,
    die,
    print_table,
    progress,
    resolve_zones,
    write_json,
)


# ── Firewall rules ───────────────────────────────────────────────────────────


async def cmd_delete_fw_rules(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Delete firewall rules, optionally filtered by description."""
    zones = await _resolve_zones(client, args)
    description = getattr(args, "description", None)
    fuzzy = getattr(args, "fuzzy", False)
    total_ok, total_fail = 0, 0

    for i, zone in enumerate(zones, 1):
        zid = zone["id"]
        rules = await client.paginate(f"/zones/{zid}/firewall/rules", per_page=1000)

        if description:
            if fuzzy:
                rules = [r for r in rules if description.lower() in r.get("description", "").lower()]
            else:
                rules = [r for r in rules if r.get("description", "") == description]

        if not rules:
            continue

        label = f"Zone {zone.get('name', zid)}"
        print(f"\n{label}: {len(rules)} firewall rules" +
              (f" matching '{description}'" + (" (fuzzy)" if fuzzy else " (exact)") if description else ""))

        if not confirm(f"Delete these {len(rules)} rules?"):
            print("  Skipped.")
            continue

        tasks = [client.delete(f"/zones/{zid}/firewall/rules/{r['id']}") for r in rules]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = sum(1 for r in results if not isinstance(r, Exception))
        fail = len(results) - ok
        total_ok += ok
        total_fail += fail
        print(f"  {ok} deleted, {fail} failed")

        progress(i, len(zones), "Zones ")

    print(f"\nTotal: {total_ok} deleted, {total_fail} failed")


async def cmd_find_filters(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Find filters matching an expression across all zones."""
    zones = await client.get_all_zones()
    expression = args.expression
    matches = []

    for i, zone in enumerate(zones, 1):
        zid = zone["id"]
        filters = await client.paginate(f"/zones/{zid}/filters", per_page=1000)
        for f in filters:
            if expression.lower() in f.get("expression", "").lower():
                matches.append({
                    "zone": zone["name"],
                    "filter_id": f["id"],
                    "expression": f["expression"],
                    "paused": f.get("paused", False),
                })
        progress(i, len(zones), "Scanning zones ")

    print(f"\nFound {len(matches)} matching filters\n")

    if args.output:
        write_json(matches, args.output)
    else:
        rows = [(m["zone"], m["filter_id"], m["expression"][:60], str(m["paused"])) for m in matches]
        print_table(rows, ["Zone", "Filter ID", "Expression", "Paused"])


# ── Rulesets ──────────────────────────────────────────────────────────────────


async def cmd_init_rulesets(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Initialize empty custom rulesets for all zones (prerequisite for migration)."""
    phase = getattr(args, "phase", "http_request_firewall_custom")
    zones = await _resolve_zones(client, args)

    if not confirm(f"Initialize '{phase}' ruleset for {len(zones)} zones?"):
        return

    ok, skip, fail = 0, 0, 0
    for i, zone in enumerate(zones, 1):
        zid = zone["id"]
        try:
            payload = {"rules": []}
            await client.put(f"/zones/{zid}/rulesets/phases/{phase}/entrypoint", json=payload)
            ok += 1
        except RuntimeError as e:
            if "404" in str(e) or "not_entitled" in str(e).lower():
                skip += 1  # zone doesn't support this phase (e.g. free plan)
            else:
                fail += 1
                print(f"  FAIL {zone.get('name', zid)}: {e}")
        except Exception as e:
            fail += 1
            print(f"  FAIL {zone.get('name', zid)}: {e}")
        progress(i, len(zones), "Init rulesets ")

    print(f"\nDone: {ok} initialized, {skip} skipped (unsupported), {fail} failed")


async def cmd_clear_ruleset(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Clear all rules in a ruleset phase for specified (or all) zones."""
    phase = args.phase
    zones = await _resolve_zones(client, args)

    if not confirm(f"Clear all rules in phase '{phase}' for {len(zones)} zones?"):
        return

    ok, fail = 0, 0
    for i, zone in enumerate(zones, 1):
        zid = zone["id"]
        try:
            data = await client.get(f"/zones/{zid}/rulesets")
            for rs in data.get("result", []):
                if rs["phase"] == phase:
                    await client.put(f"/zones/{zid}/rulesets/{rs['id']}", json={"rules": []})
                    ok += 1
                    break
        except Exception as e:
            fail += 1
            print(f"  FAIL {zone.get('name', zid)}: {e}")
        progress(i, len(zones), "Clearing rulesets ")

    print(f"\nDone: {ok} cleared, {fail} failed")


async def cmd_purge_versions(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Delete old (non-latest) ruleset versions for a given phase across all zones."""
    phase = args.phase
    zones = await _resolve_zones(client, args)
    total_deleted = 0

    for i, zone in enumerate(zones, 1):
        zid = zone["id"]
        try:
            data = await client.get(f"/zones/{zid}/rulesets")
            for rs in data.get("result", []):
                if rs["phase"] != phase:
                    continue
                rid = rs["id"]
                ver_data = await client.get(f"/zones/{zid}/rulesets/{rid}/versions")
                versions = ver_data.get("result", [])
                # Skip the first (latest) version
                for ver in versions[1:]:
                    vid = ver["version"]
                    try:
                        await client.delete(f"/zones/{zid}/rulesets/{rid}/versions/{vid}")
                        total_deleted += 1
                    except Exception:
                        pass
        except Exception as e:
            print(f"  Warning: {zone.get('name', zid)}: {e}")
        progress(i, len(zones), "Purging versions ")

    print(f"\nDeleted {total_deleted} old versions")


async def cmd_revert(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Revert a ruleset to a previous or specific version.

    Interactive: prompts for zone, phase/ruleset, and version if not provided.
    """
    zone_id = args.zone_id or input("Enter zone_id: ").strip()
    if not zone_id:
        die("Zone ID is required.")

    # List rulesets for this zone
    data = await client.get(f"/zones/{zone_id}/rulesets")
    rulesets = data.get("result", [])

    if args.ruleset_id:
        ruleset_id = args.ruleset_id
    elif args.phase:
        # Find ruleset by phase
        source_filter = args.source  # 'firewall_custom' or 'firewall_managed'
        candidates = [
            rs for rs in rulesets
            if rs["phase"] == args.phase
            and (not source_filter or rs.get("source") == source_filter)
        ]
        if not candidates:
            die(f"No ruleset found for phase '{args.phase}'" +
                (f" with source '{source_filter}'" if source_filter else ""))
        if len(candidates) > 1:
            print("Multiple rulesets found:")
            for rs in candidates:
                print(f"  {rs['id']} - {rs.get('name', 'N/A')} (source: {rs.get('source', 'N/A')})")
            ruleset_id = input("Enter ruleset ID: ").strip()
        else:
            ruleset_id = candidates[0]["id"]
    else:
        # Show all phases and let user pick
        print("\nAvailable rulesets:")
        rows = [(rs["id"], rs["phase"], rs.get("name", ""), rs.get("source", "")) for rs in rulesets]
        print_table(rows, ["Ruleset ID", "Phase", "Name", "Source"])
        ruleset_id = input("\nEnter ruleset ID: ").strip()

    # Get versions
    ver_data = await client.get(f"/zones/{zone_id}/rulesets/{ruleset_id}/versions")
    versions = ver_data.get("result", [])

    if not versions:
        die("No versions found.")

    if args.version:
        target_version = args.version
    else:
        print("\nAvailable versions:")
        for idx, v in enumerate(versions):
            marker = " (latest)" if idx == 0 else ""
            print(f"  [{idx}] Version {v['version']}{marker}")

        if len(versions) < 2:
            die("Only one version exists, nothing to revert to.")

        choice = input(f"Revert to version [default: previous ({versions[1]['version']})]: ").strip()
        target_version = choice if choice else versions[1]["version"]

    # Fetch that version's rules
    ver_detail = await client.get(
        f"/zones/{zone_id}/rulesets/{ruleset_id}/versions/{target_version}"
    )
    rules_raw = ver_detail.get("result", {}).get("rules", [])

    # Clean rules for PUT -- strip server-generated IDs but preserve all
    # functional fields (especially important for managed rulesets which
    # may have ref, categories, etc.)
    SERVER_ONLY_KEYS = {"id", "version", "last_updated"}
    clean_rules = []
    for rule in rules_raw:
        cleaned = {k: v for k, v in rule.items() if k not in SERVER_ONLY_KEYS}
        clean_rules.append(cleaned)

    print(f"\nReverting to version {target_version} ({len(clean_rules)} rules)")
    if not confirm("Proceed?"):
        return

    await client.put(f"/zones/{zone_id}/rulesets/{ruleset_id}", json={"rules": clean_rules})
    print("Reverted successfully.")


# ── Migration ─────────────────────────────────────────────────────────────────


async def cmd_migrate(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Migrate legacy firewall rules to custom rules (WAF custom rulesets).

    For each zone: reads firewall rules, transforms them (bypass->skip, allow->skip),
    prepends them to existing custom rules, and PUTs the merged set.
    """
    zones = await _resolve_zones(client, args)
    dry_run = getattr(args, "dry_run", False)

    for zone in zones:
        zid = zone["id"]
        zname = zone.get("name", zid)

        # 1. Get firewall rules
        fw_rules = await client.paginate(f"/zones/{zid}/firewall/rules", per_page=1000)
        if not fw_rules:
            print(f"{zname}: No firewall rules to migrate")
            continue

        # 2. Transform to custom rule format
        new_rules = []
        for fr in fw_rules:
            filt = fr.get("filter") or {}
            if not filt.get("expression"):
                print(f"  Skipping rule {fr.get('id', '?')}: no filter expression")
                continue
            rule = {
                "description": fr.get("description", "") + " (Firewall Rule)",
                "action": fr["action"],
                "expression": filt["expression"],
                "enabled": not fr.get("paused", False),
            }
            # bypass -> skip with products
            if rule["action"] == "bypass":
                rule["action"] = "skip"
                rule["action_parameters"] = {"products": fr.get("products", [])}
            # allow -> skip current ruleset
            elif rule["action"] == "allow":
                rule["action"] = "skip"
                rule["action_parameters"] = {"ruleset": "current"}

            new_rules.append(rule)

        # 3. Get existing custom ruleset
        rs_data = await client.get(f"/zones/{zid}/rulesets")
        ruleset_id = None
        for rs in rs_data.get("result", []):
            if (rs["phase"] == "http_request_firewall_custom"
                    and rs.get("source") == "firewall_custom"):
                ruleset_id = rs["id"]
                break

        if not ruleset_id:
            print(f"{zname}: No custom ruleset found (run 'init-rulesets' first)")
            continue

        # Get existing rules
        existing_data = await client.get(f"/zones/{zid}/rulesets/{ruleset_id}")
        existing_rules_raw = existing_data.get("result", {}).get("rules", [])
        existing_rules = []
        for rule in existing_rules_raw:
            cleaned = {
                "action": rule["action"],
                "expression": rule["expression"],
                "description": rule.get("description", ""),
                "enabled": rule.get("enabled", True),
            }
            if "logging" in rule:
                cleaned["logging"] = rule["logging"]
            if "action_parameters" in rule:
                cleaned["action_parameters"] = rule["action_parameters"]
            existing_rules.append(cleaned)

        merged = new_rules + existing_rules
        print(f"{zname}: {len(fw_rules)} firewall rules + {len(existing_rules)} existing = {len(merged)} total")

        if dry_run:
            print("  [DRY RUN] No changes made.")
            continue

        if not confirm(f"  Apply migration for {zname}?"):
            continue

        await client.put(f"/zones/{zid}/rulesets/{ruleset_id}", json={"rules": merged})
        print(f"  Migrated successfully.")


async def cmd_tf_import(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Import Cloudflare rulesets into Terraform state.

    For each zone+ruleset matching the phase, runs:
      terraform import cloudflare_ruleset.<zone_id> <zone_id>/<ruleset_id>
    """
    phase = args.phase
    zones = await _resolve_zones(client, args)

    for zone in zones:
        zid = zone["id"]
        data = await client.get(f"/zones/{zid}/rulesets")
        for rs in data.get("result", []):
            if rs["phase"] != phase:
                continue
            rid = rs["id"]
            resource = f"cloudflare_ruleset.my_ruleset_{rid}"
            import_id = f"{zid}/{rid}"
            print(f"Importing {resource} = {import_id}")
            try:
                subprocess.run(
                    ["terraform", "import", resource, import_id],
                    check=True,
                    capture_output=not args.verbose,
                )
            except subprocess.CalledProcessError as e:
                print(f"  FAIL: {e}")
            except FileNotFoundError:
                die("terraform not found in PATH")


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _resolve_zones(client, args, require_ids=False):
    return await resolve_zones(client, args, require_ids=require_ids)


# ── CLI definition ────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cf_security",
        description="Cloudflare Security Rules & Rulesets CLI",
    )
    add_auth_args(parser)
    sub = parser.add_subparsers(dest="command", help="Subcommand")

    # delete-fw-rules
    p = sub.add_parser("delete-fw-rules", help="Delete firewall rules")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--description", help="Only delete rules matching this description (exact match)")
    p.add_argument("--fuzzy", action="store_true", help="Use substring match instead of exact for --description")

    # find-filters
    p = sub.add_parser("find-filters", help="Find filters by expression")
    p.add_argument("--expression", required=True, help="Expression substring to search")
    p.add_argument("--output", "-o", help="Output file (JSON)")

    # init-rulesets
    p = sub.add_parser("init-rulesets", help="Initialize empty custom rulesets")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--phase", default="http_request_firewall_custom", help="Phase name")

    # clear-ruleset
    p = sub.add_parser("clear-ruleset", help="Clear all rules in a ruleset phase")
    p.add_argument("--phase", required=True, help="Ruleset phase")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")

    # purge-versions
    p = sub.add_parser("purge-versions", help="Delete old ruleset versions")
    p.add_argument("--phase", required=True, help="Ruleset phase")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")

    # revert
    p = sub.add_parser("revert", help="Revert a ruleset to a previous version")
    p.add_argument("--zone-id", help="Zone ID")
    p.add_argument("--phase", help="Ruleset phase (to auto-find ruleset)")
    p.add_argument("--source", help="Ruleset source filter (firewall_custom, firewall_managed)")
    p.add_argument("--ruleset-id", help="Specific ruleset ID")
    p.add_argument("--version", help="Specific version to revert to")

    # migrate
    p = sub.add_parser("migrate", help="Migrate firewall rules to custom rules")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--dry-run", action="store_true", help="Preview without applying")

    # tf-import
    p = sub.add_parser("tf-import", help="Import rulesets into Terraform state")
    p.add_argument("--phase", required=True, help="Ruleset phase")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--verbose", "-v", action="store_true", help="Show terraform output")

    return parser


COMMANDS = {
    "delete-fw-rules": cmd_delete_fw_rules,
    "find-filters": cmd_find_filters,
    "init-rulesets": cmd_init_rulesets,
    "clear-ruleset": cmd_clear_ruleset,
    "purge-versions": cmd_purge_versions,
    "revert": cmd_revert,
    "migrate": cmd_migrate,
    "tf-import": cmd_tf_import,
}


async def run(args: argparse.Namespace) -> None:
    auth = AuthConfig.from_args_or_env(
        token=args.api_token, email=args.email, api_key=args.api_key,
    )
    print(f"Auth: {auth.mode}")
    handler = COMMANDS[args.command]
    async with CloudflareClient(auth) as client:
        await handler(client, args)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(130)
    except Exception as e:
        die(str(e))


if __name__ == "__main__":
    main()
