#!/usr/bin/env python3
"""
Cloudflare Access & Zero Trust CLI

Consolidates Access application, IDP, service token, account member, and
Zero Trust user/seat management into a single async tool.

Replaces:
  - delete_all_access_apps.py
  - delete_all_idps.py
  - delete_all_service_tokens.py
  - list_members.py
  - async_get_all_active_sessions.py
  - async_remove_inactive_users.py
  - async_zt_seat_manager.py

Usage:
    python cf_access.py apps --account-ids ACCT1,ACCT2
    python cf_access.py apps --account-ids ACCT1 --delete
    python cf_access.py idps --account-ids ACCT1 --delete
    python cf_access.py tokens --account-ids ACCT1 --delete
    python cf_access.py members
    python cf_access.py members --output members.csv
    python cf_access.py users --account-id ACCT1
    python cf_access.py users --account-id ACCT1 --delete-inactive
    python cf_access.py users --account-id ACCT1 --remove-seats
"""

import argparse
import asyncio
import csv
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from cf_lib import CloudflareClient, AuthConfig
from cf_lib.output import (
    add_auth_args,
    confirm,
    die,
    print_table,
    progress,
    timestamp_filename,
    write_csv,
    write_json,
)


# ── Data models for Zero Trust ────────────────────────────────────────────────


@dataclass
class UserRecord:
    id: str
    name: str
    email: str
    active_sessions: int = 0
    access_seat: bool = False
    gateway_seat: bool = False
    session_expiration: str = "N/A"
    last_issued: str = "N/A"
    created_at: str = ""
    last_successful_login: str = ""
    app_count: int = 0


# ── Account-resource subcommands (apps, idps, tokens) ────────────────────────


async def cmd_apps(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List or delete Access applications."""
    account_ids = _get_account_ids(args)

    for acct_id in account_ids:
        apps = await client.paginate(f"/accounts/{acct_id}/access/apps")
        print(f"\nAccount {acct_id}: {len(apps)} Access apps")

        if not apps:
            continue

        rows = [(a["id"], a.get("name", ""), a.get("type", ""), a.get("domain", "")) for a in apps]
        print_table(rows, ["App ID", "Name", "Type", "Domain"])

        if args.delete:
            if not confirm(f"Delete all {len(apps)} Access apps?"):
                continue
            ok, fail = await _bulk_delete(
                client, [f"/accounts/{acct_id}/access/apps/{a['id']}" for a in apps]
            )
            print(f"  {ok} deleted, {fail} failed")


async def cmd_idps(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List or delete Identity Providers."""
    account_ids = _get_account_ids(args)

    for acct_id in account_ids:
        idps = await client.paginate(f"/accounts/{acct_id}/access/identity_providers")
        print(f"\nAccount {acct_id}: {len(idps)} Identity Providers")

        if not idps:
            continue

        rows = [(i["id"], i.get("name", ""), i.get("type", "")) for i in idps]
        print_table(rows, ["IDP ID", "Name", "Type"])

        if args.delete:
            if not confirm(f"Delete all {len(idps)} IDPs?"):
                continue
            ok, fail = await _bulk_delete(
                client,
                [f"/accounts/{acct_id}/access/identity_providers/{i['id']}" for i in idps],
            )
            print(f"  {ok} deleted, {fail} failed")


async def cmd_tokens(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List or delete service tokens."""
    account_ids = _get_account_ids(args)

    for acct_id in account_ids:
        tokens = await client.paginate(f"/accounts/{acct_id}/access/service_tokens")
        print(f"\nAccount {acct_id}: {len(tokens)} service tokens")

        if not tokens:
            continue

        rows = [
            (t["id"], t.get("name", ""), t.get("expires_at", "N/A"))
            for t in tokens
        ]
        print_table(rows, ["Token ID", "Name", "Expires"])

        if args.delete:
            if not confirm(f"Delete all {len(tokens)} service tokens?"):
                continue
            ok, fail = await _bulk_delete(
                client,
                [f"/accounts/{acct_id}/access/service_tokens/{t['id']}" for t in tokens],
            )
            print(f"  {ok} deleted, {fail} failed")


# ── Members ───────────────────────────────────────────────────────────────────


async def cmd_members(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List all account members across all accounts."""
    accounts = await client.get_all_accounts()
    print(f"Found {len(accounts)} accounts")

    all_rows = []

    async def fetch_members(acct: dict) -> list[dict]:
        aid = acct["id"]
        members = await client.paginate(f"/accounts/{aid}/members", per_page=50)
        return [
            {
                "Account ID": aid,
                "Account Name": acct.get("name", ""),
                "Member Email": _safe_email(m),
                "Status": m.get("status", ""),
                "Roles": ", ".join(
                    r.get("name", str(r))
                    for r in (m.get("roles") or [])
                    if isinstance(r, dict)
                ),
            }
            for m in members
            if isinstance(m, dict)
        ]

    tasks = [fetch_members(a) for a in accounts]
    for i, result in enumerate(asyncio.as_completed(tasks), 1):
        all_rows.extend(await result)
        progress(i, len(accounts), "Fetching members ")

    print(f"\nTotal members: {len(all_rows)}\n")

    if args.output:
        hdrs = ["Account ID", "Account Name", "Member Email", "Status", "Roles"]
        if args.output.endswith(".csv"):
            write_csv(all_rows, hdrs, args.output)
        else:
            write_json(all_rows, args.output)
    else:
        rows = [
            (r["Account Name"][:25], r["Member Email"][:35], r["Status"], r["Roles"][:30])
            for r in all_rows
        ]
        print_table(rows, ["Account", "Email", "Status", "Roles"])


# ── Zero Trust users ──────────────────────────────────────────────────────────


async def cmd_users(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Audit Zero Trust users: list sessions, delete inactive, remove seats."""
    account_id = args.account_id or os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    if not account_id:
        account_id = input("Enter Cloudflare account ID: ").strip()
    if not account_id:
        die("Account ID is required.")

    base = f"/accounts/{account_id}"

    # Fetch all users
    print("Fetching Zero Trust users...")
    users_raw = await client.paginate(f"{base}/access/users", per_page=50)
    print(f"Found {len(users_raw)} users, enriching with session data...")

    # Enrich with session info
    async def enrich(u: dict) -> UserRecord:
        uid = u["id"]
        sess_data = await client.get(f"{base}/access/users/{uid}/active_sessions")
        sessions = sess_data.get("result") or []
        rec = UserRecord(
            id=uid,
            name=u.get("name") or u.get("email", "unknown"),
            email=u.get("email", "unknown"),
            access_seat=u.get("access_seat", False),
            gateway_seat=u.get("gateway_seat", False),
            active_sessions=len(sessions),
            created_at=u.get("created_at", ""),
            last_successful_login=u.get("last_successful_login", ""),
        )
        if sessions:
            first = sessions[0]
            exp = first.get("expiration")
            rec.session_expiration = _fmt_ts(exp)
            meta = first.get("metadata") or {}
            rec.last_issued = _fmt_ts(meta.get("iat"))
            rec.app_count = len(meta.get("apps") or {})
        return rec

    records = await asyncio.gather(*[enrich(u) for u in users_raw])
    records = list(records)

    # Print summary
    active = sum(1 for r in records if r.active_sessions > 0)
    inactive = len(records) - active
    seated = sum(1 for r in records if r.access_seat or r.gateway_seat)

    print(f"\n{'='*60}")
    print(f"  Total users:     {len(records)}")
    print(f"  Active sessions: {active}")
    print(f"  No sessions:     {inactive}")
    print(f"  Seated:          {seated}")
    print(f"{'='*60}\n")

    rows = [
        (r.name[:25], r.email[:30], str(r.active_sessions),
         "Y" if r.access_seat else "-", "Y" if r.gateway_seat else "-",
         str(r.app_count))
        for r in sorted(records, key=lambda x: x.active_sessions, reverse=True)
    ]
    print_table(rows, ["Name", "Email", "Sessions", "Access", "GW", "Apps"])

    # Export
    if args.output:
        data = [asdict(r) for r in records]
        if args.output.endswith(".csv"):
            hdrs = list(data[0].keys()) if data else []
            write_csv(data, hdrs, args.output)
        else:
            write_json(data, args.output)

    # Delete inactive users
    if args.delete_inactive:
        targets = [r for r in records if r.active_sessions == 0]
        if not targets:
            print("\nNo inactive users found.")
            return

        print(f"\n{len(targets)} inactive users:")
        for r in targets:
            print(f"  {r.email}")

        if args.dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not confirm(f"\nDelete {len(targets)} inactive users?"):
            return

        ok, fail = await _bulk_delete(
            client, [f"{base}/access/users/{r.id}" for r in targets]
        )
        print(f"Done: {ok} deleted, {fail} failed")

    # Remove seats
    if args.remove_seats:
        targets = [r for r in records if r.access_seat or r.gateway_seat]
        if not targets:
            print("\nNo seated users found.")
            return

        if args.inactive_only:
            targets = [r for r in targets if r.active_sessions == 0]

        print(f"\n{len(targets)} users with seats to clear:")
        for r in targets:
            seats = []
            if r.access_seat:
                seats.append("access")
            if r.gateway_seat:
                seats.append("gateway")
            print(f"  {r.email} [{', '.join(seats)}]")

        if args.dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not confirm(f"\nRemove seats for {len(targets)} users?"):
            return

        ok, fail = 0, 0
        for r in targets:
            payload = [{"seat_uid": r.id, "access_seat": False, "gateway_seat": False}]
            try:
                result = await client.patch(f"{base}/access/seats", json=payload)
                if result.get("success"):
                    ok += 1
                else:
                    fail += 1
                    errors = result.get("errors", [])
                    print(f"  FAIL {r.email}: {errors}")
            except Exception as e:
                fail += 1
                print(f"  FAIL {r.email}: {e}")
        print(f"Done: {ok} cleared, {fail} failed")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_account_ids(args: argparse.Namespace) -> list[str]:
    """Get account IDs from args or prompt."""
    ids_str = getattr(args, "account_ids", None)
    if ids_str:
        return [a.strip() for a in ids_str.split(",")]
    raw = input("Enter account IDs (comma-separated): ").strip()
    if not raw:
        die("No account IDs provided.")
    return [a.strip() for a in raw.split(",")]


async def _bulk_delete(client: CloudflareClient, paths: list[str]) -> tuple[int, int]:
    """Delete resources at the given paths, return (ok, fail) counts."""
    tasks = [client.delete(p) for p in paths]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok = sum(1 for r in results if not isinstance(r, Exception))
    fail = len(results) - ok
    return ok, fail


def _safe_email(member: dict) -> str:
    user = member.get("user")
    if isinstance(user, dict):
        return user.get("email", "")
    return ""


def _fmt_ts(ts: Any) -> str:
    if ts is None:
        return "N/A"
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if isinstance(ts, str) and ts:
        return ts
    return "N/A"


# ── CLI definition ────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cf_access",
        description="Cloudflare Access & Zero Trust CLI",
    )
    add_auth_args(parser)
    sub = parser.add_subparsers(dest="command", help="Subcommand")

    # apps
    p = sub.add_parser("apps", help="List or delete Access applications")
    p.add_argument("--account-ids", help="Comma-separated account IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed apps")

    # idps
    p = sub.add_parser("idps", help="List or delete Identity Providers")
    p.add_argument("--account-ids", help="Comma-separated account IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed IDPs")

    # tokens
    p = sub.add_parser("tokens", help="List or delete service tokens")
    p.add_argument("--account-ids", help="Comma-separated account IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed tokens")

    # members
    p = sub.add_parser("members", help="List account members")
    p.add_argument("--output", "-o", help="Output file (.csv or .json)")

    # users
    p = sub.add_parser("users", help="Audit Zero Trust users and sessions")
    p.add_argument("--account-id", help="Account ID (or set CLOUDFLARE_ACCOUNT_ID)")
    p.add_argument("--output", "-o", help="Export to file (.csv or .json)")
    p.add_argument("--delete-inactive", action="store_true",
                   help="Delete users with no active sessions")
    p.add_argument("--remove-seats", action="store_true",
                   help="Clear seat assignments")
    p.add_argument("--inactive-only", action="store_true",
                   help="With --remove-seats, only target inactive users")
    p.add_argument("--dry-run", action="store_true",
                   help="Preview changes without applying")

    return parser


COMMANDS = {
    "apps": cmd_apps,
    "idps": cmd_idps,
    "tokens": cmd_tokens,
    "members": cmd_members,
    "users": cmd_users,
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
