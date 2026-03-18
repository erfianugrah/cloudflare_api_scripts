#!/usr/bin/env python3
"""
Cloudflare DNS & Zone Management CLI

Consolidates zone, DNS, hostname, cert, DMARC, pending-zone, and Spectrum
operations into a single async tool.

Replaces:
  - list_all_zone_ids_in_a_list.py
  - list_all_proxied_dns_records.py
  - delete_all_dns_records.py
  - list_all_custom_hostnames.py  /  async_list_all_custom_hostnames.py
  - list_non_active_certs.py
  - fetch_all_pending_zones_txt.py
  - enable_dmarc_all_zones.py
  - delete_all_spectrum_apps.py

Usage:
    python cf_dns.py zones
    python cf_dns.py records --proxied-only
    python cf_dns.py delete-records --zone-ids abc123,def456
    python cf_dns.py hostnames
    python cf_dns.py certs
    python cf_dns.py pending --account ACCT_ID
    python cf_dns.py dmarc
    python cf_dns.py spectrum --zone-ids abc123 --delete
"""

import argparse
import asyncio
import json
import sys

from cf_lib import CloudflareClient, AuthConfig
from cf_lib.output import (
    add_auth_args,
    confirm,
    die,
    print_table,
    progress,
    resolve_zones,
    timestamp_filename,
    write_csv,
    write_json,
)


# ── Subcommand handlers ──────────────────────────────────────────────────────


async def cmd_zones(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List all zones."""
    def on_page(p, total):
        progress(p, total, "Fetching zones ")

    zones = await client.get_all_zones(on_page=on_page)
    print(f"\nFound {len(zones)} zones\n")

    if args.json:
        write_json([{"id": z["id"], "name": z["name"], "status": z["status"]} for z in zones])
    else:
        rows = [(z["id"], z["name"], z["status"]) for z in zones]
        print_table(rows, ["Zone ID", "Name", "Status"])


async def cmd_records(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List DNS records across zones (optionally only proxied)."""
    zones = await _resolve_zones(client, args)
    all_records = []

    async def fetch_records(zone: dict) -> list[dict]:
        zid = zone["id"]
        recs = await client.paginate(f"/zones/{zid}/dns_records", per_page=1000)
        if args.proxied_only:
            recs = [r for r in recs if r.get("proxied")]
        for r in recs:
            r["_zone_name"] = zone["name"]
        return recs

    tasks = [fetch_records(z) for z in zones]
    for i, result in enumerate(asyncio.as_completed(tasks), 1):
        recs = await result
        all_records.extend(recs)
        progress(i, len(zones), "Scanning zones ")

    print(f"\nFound {len(all_records)} {'proxied ' if args.proxied_only else ''}records\n")

    if args.output:
        write_json(all_records, args.output)
    else:
        rows = [
            (r["_zone_name"], r["type"], r["name"], r.get("content", "")[:40], str(r.get("proxied", "")))
            for r in all_records
        ]
        print_table(rows, ["Zone", "Type", "Name", "Content", "Proxied"])


async def cmd_delete_records(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Delete all DNS records in specified zones."""
    zones = await _resolve_zones(client, args, require_ids=True)

    for zone in zones:
        zid = zone["id"]
        records = await client.paginate(f"/zones/{zid}/dns_records", per_page=1000)
        print(f"\nZone {zone['name']}: {len(records)} records")

        if not records:
            continue
        if not confirm(f"Delete all {len(records)} records in {zone['name']}?"):
            print("  Skipped.")
            continue

        ok, fail = 0, 0
        tasks = [client.delete(f"/zones/{zid}/dns_records/{r['id']}") for r in records]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for rec, res in zip(records, results):
            if isinstance(res, Exception):
                fail += 1
                print(f"  FAIL {rec['type']} {rec['name']}: {res}")
            else:
                ok += 1
        print(f"  Done: {ok} deleted, {fail} failed")


async def cmd_hostnames(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List custom hostnames across zones."""
    zones = await _resolve_zones(client, args)
    all_hostnames = []

    async def fetch_hostnames(zone: dict) -> list[dict]:
        zid = zone["id"]
        hns = await client.paginate(f"/zones/{zid}/custom_hostnames")
        for h in hns:
            h["_zone_name"] = zone["name"]
        return hns

    tasks = [fetch_hostnames(z) for z in zones]
    for i, result in enumerate(asyncio.as_completed(tasks), 1):
        hns = await result
        all_hostnames.extend(hns)
        progress(i, len(zones), "Scanning zones ")

    print(f"\nFound {len(all_hostnames)} custom hostnames\n")

    if args.output:
        write_json(all_hostnames, args.output)
    else:
        rows = [
            (h["_zone_name"], h.get("hostname", ""), h.get("status", ""),
             h.get("ssl", {}).get("status", ""))
            for h in all_hostnames
        ]
        print_table(rows, ["Zone", "Hostname", "Status", "SSL Status"])


async def cmd_certs(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List non-active certificate packs across zones."""
    zones = await _resolve_zones(client, args)
    non_active = []

    async def fetch_certs(zone: dict) -> list[dict]:
        zid = zone["id"]
        packs = await client.paginate(
            f"/zones/{zid}/ssl/certificate_packs",
            params={"status": "all"},
        )
        return [
            {**p, "_zone_name": zone["name"]}
            for p in packs
            if p.get("status") != "active"
        ]

    tasks = [fetch_certs(z) for z in zones]
    for i, result in enumerate(asyncio.as_completed(tasks), 1):
        non_active.extend(await result)
        progress(i, len(zones), "Scanning zones ")

    print(f"\nFound {len(non_active)} non-active certificate packs\n")

    if args.output:
        write_json(non_active, args.output)
    else:
        rows = [
            (c["_zone_name"], c.get("id", ""), c.get("type", ""), c.get("status", ""))
            for c in non_active
        ]
        print_table(rows, ["Zone", "Pack ID", "Type", "Status"])


async def cmd_pending(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Fetch pending zones with TXT verification and DCV delegation info."""
    # Get accounts
    if args.account:
        account_ids = [args.account]
        acct_names = {args.account: args.account}
    else:
        accounts = await client.get_all_accounts()
        account_ids = [a["id"] for a in accounts]
        acct_names = {a["id"]: a.get("name", a["id"]) for a in accounts}
        print(f"Found {len(accounts)} accounts")

    records = []
    pending_count = 0

    for acct_id in account_ids:
        acct_name = acct_names.get(acct_id, acct_id)
        zones = await client.paginate("/zones", params={"account.id": acct_id}, per_page=1000)
        pending = [z for z in zones if z.get("status") != "active"]
        pending_count += len(pending)
        if not pending:
            continue

        print(f"Account {acct_name}: {len(pending)} pending zones")

        for zone in pending:
            zid, zname = zone["id"], zone["name"]
            status = zone["status"]

            # TXT verification
            zdetail = await client.get(f"/zones/{zid}")
            vkey = zdetail.get("result", {}).get("verification_key")
            if vkey:
                records.append({
                    "Zone": zname, "Status": status, "Account": acct_name,
                    "Type": "TXT", "Record Name": f"cloudflare-verify.{zname}",
                    "Record Type": "TXT", "Value": vkey,
                })

            # DCV delegation
            try:
                dcv = await client.get(f"/zones/{zid}/dcv_delegation/uuid")
                uuid = dcv.get("result", {}).get("uuid")
                if uuid:
                    dns_recs = []
                    for rtype in ("A", "AAAA", "CNAME"):
                        data = await client.get(f"/zones/{zid}/dns_records", params={"type": rtype})
                        dns_recs.extend(data.get("result", []))

                    for dr in dns_recs:
                        hostname = dr["name"]
                        if hostname.endswith(f".{zname}"):
                            hostname = hostname[: -len(zname) - 1]
                        if not hostname:
                            hostname = "@"
                        acme_name = f"_acme-challenge.{hostname}"
                        acme_val = (
                            f"{zname}.{uuid}.dcv.cloudflare.com"
                            if hostname == "@"
                            else f"{hostname}.{uuid}.dcv.cloudflare.com"
                        )
                        records.append({
                            "Zone": zname, "Status": status, "Account": acct_name,
                            "Type": "DCV", "Record Name": acme_name,
                            "Record Type": "CNAME", "Value": acme_val,
                        })
            except Exception as e:
                print(f"  Warning: DCV fetch failed for {zname}: {e}")

    print(f"\nTotal pending zones: {pending_count}")
    print(f"Verification records: {len(records)}\n")

    if args.output:
        if args.output.endswith(".csv"):
            hdrs = ["Zone", "Status", "Account", "Type", "Record Name", "Record Type", "Value"]
            write_csv(records, hdrs, args.output)
        else:
            write_json(records, args.output)
    else:
        rows = [(r["Zone"], r.get("Account", ""), r["Type"], r["Record Name"], r["Record Type"], r["Value"][:60]) for r in records]
        print_table(rows, ["Zone", "Account", "Type", "Record Name", "Record Type", "Value"])


async def cmd_dmarc(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Enable DMARC reporting for all zones."""
    zones = await _resolve_zones(client, args)
    print(f"Enabling DMARC for {len(zones)} zones...")

    ok, fail = 0, 0
    for i, zone in enumerate(zones, 1):
        try:
            await client.patch(
                f"/zones/{zone['id']}/email/security/dmarc-reports",
                json={"enabled": True},
            )
            ok += 1
        except Exception as e:
            fail += 1
            print(f"  FAIL {zone['name']}: {e}")
        progress(i, len(zones), "DMARC ")

    print(f"Done: {ok} enabled, {fail} failed")


async def cmd_spectrum(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List or delete Spectrum applications for specified zones."""
    zones = await _resolve_zones(client, args, require_ids=True)

    for zone in zones:
        zid = zone["id"]
        apps = await client.paginate(f"/zones/{zid}/spectrum/apps")
        print(f"\nZone {zone['name']}: {len(apps)} Spectrum apps")

        if not apps:
            continue

        rows = [(a["id"], a.get("protocol", ""), a.get("dns", {}).get("name", "")) for a in apps]
        print_table(rows, ["App ID", "Protocol", "DNS Name"])

        if args.delete:
            if not confirm(f"Delete all {len(apps)} Spectrum apps in {zone['name']}?"):
                print("  Skipped.")
                continue
            ok, fail = 0, 0
            tasks = [client.delete(f"/zones/{zid}/spectrum/apps/{a['id']}") for a in apps]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for app, res in zip(apps, results):
                if isinstance(res, Exception):
                    fail += 1
                    print(f"  FAIL {app['id']}: {res}")
                else:
                    ok += 1
            print(f"  Done: {ok} deleted, {fail} failed")


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _resolve_zones(client, args, require_ids=False):
    return await resolve_zones(client, args, require_ids=require_ids)


# ── CLI definition ────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cf_dns",
        description="Cloudflare DNS & Zone Management CLI",
    )
    add_auth_args(parser)
    sub = parser.add_subparsers(dest="command", help="Subcommand")

    # zones
    p = sub.add_parser("zones", help="List all zones")
    p.add_argument("--json", action="store_true", help="Output as JSON")

    # records
    p = sub.add_parser("records", help="List DNS records")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--proxied-only", action="store_true", help="Only proxied records")
    p.add_argument("--output", "-o", help="Write results to file (JSON)")

    # delete-records
    p = sub.add_parser("delete-records", help="Delete all DNS records in zones")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs")

    # hostnames
    p = sub.add_parser("hostnames", help="List custom hostnames")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--output", "-o", help="Write results to file (JSON)")

    # certs
    p = sub.add_parser("certs", help="List non-active certificate packs")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--output", "-o", help="Write results to file (JSON)")

    # pending
    p = sub.add_parser("pending", help="Fetch pending zones with verification info")
    p.add_argument("--account", "-a", help="Single account ID to check")
    p.add_argument("--output", "-o", help="Output file (.csv or .json)")

    # dmarc
    p = sub.add_parser("dmarc", help="Enable DMARC reporting for zones")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")

    # spectrum
    p = sub.add_parser("spectrum", help="List or delete Spectrum apps")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed apps")

    return parser


COMMANDS = {
    "zones": cmd_zones,
    "records": cmd_records,
    "delete-records": cmd_delete_records,
    "hostnames": cmd_hostnames,
    "certs": cmd_certs,
    "pending": cmd_pending,
    "dmarc": cmd_dmarc,
    "spectrum": cmd_spectrum,
}


async def run(args: argparse.Namespace) -> None:
    auth = AuthConfig.from_args_or_env(
        token=args.api_token,
        email=args.email,
        api_key=args.api_key,
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
