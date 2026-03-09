"""
Cloudflare Zero Trust User & Seat Management Tool

Combines user auditing, session inspection, and bulk user/seat operations
into a single interactive CLI. Fixes: proper pagination, rate limiting,
error handling, dry-run support.

Usage (API Key + Email):
    export CLOUDFLARE_API_KEY="your-api-key"
    export CLOUDFLARE_EMAIL="your-email"
    export CLOUDFLARE_ACCOUNT_ID="your-account-id"  # optional
    python cf_zt_manager.py

Usage (Bearer Token):
    export CLOUDFLARE_API_TOKEN="your-bearer-token"
    export CLOUDFLARE_ACCOUNT_ID="your-account-id"  # optional
    python cf_zt_manager.py

If both are set, Bearer token takes precedence.
"""

import asyncio
import aiohttp
import os
import sys
import csv
import json
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.cloudflare.com/client/v4/accounts"
MAX_CONCURRENT = 10  # stay well under API rate limits
PER_PAGE = 50        # CF Access users endpoint real max

# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class AppInfo:
    name: str = "N/A"
    hostname: str = "N/A"
    app_type: str = "N/A"
    uid: str = "N/A"


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
    apps: List[AppInfo] = field(default_factory=list)
    created_at: str = ""
    last_successful_login: str = ""


# ── API layer ─────────────────────────────────────────────────────────────────

class CloudflareClient:
    def __init__(
        self,
        account_id: str,
        api_token: Optional[str] = None,
        api_key: Optional[str] = None,
        email: Optional[str] = None,
    ):
        self.account_id = account_id
        self.base = f"{BASE_URL}/{account_id}"
        self.headers = {"Content-Type": "application/json"}

        if api_token:
            self.headers["Authorization"] = f"Bearer {api_token}"
            self.auth_mode = "bearer"
        elif api_key and email:
            self.headers["X-Auth-Key"] = api_key
            self.headers["X-Auth-Email"] = email
            self.auth_mode = "key+email"
        else:
            raise ValueError(
                "Provide either CLOUDFLARE_API_TOKEN or both "
                "CLOUDFLARE_API_KEY + CLOUDFLARE_EMAIL"
            )

        self.sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def _request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        payload: Optional[Dict] = None,
    ) -> Dict:
        async with self.sem:
            kwargs = {"headers": self.headers}
            if payload is not None:
                kwargs["json"] = payload
            async with session.request(method, url, **kwargs) as resp:
                data = await resp.json()
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    print(f"  ⏳ Rate limited, waiting {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    return await self._request(session, method, url, payload)
                return data

    async def get(self, session: aiohttp.ClientSession, path: str) -> Dict:
        return await self._request(session, "GET", f"{self.base}/{path}")

    async def delete(self, session: aiohttp.ClientSession, path: str) -> Dict:
        return await self._request(session, "DELETE", f"{self.base}/{path}")

    async def patch(
        self, session: aiohttp.ClientSession, path: str, payload: Dict
    ) -> Dict:
        return await self._request(session, "PATCH", f"{self.base}/{path}", payload)

    # ── Paginated fetch ───────────────────────────────────────────────────

    async def get_all_users(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch all Access users with proper pagination."""
        users = []
        page = 1
        while True:
            data = await self.get(
                session, f"access/users?per_page={PER_PAGE}&page={page}"
            )
            if not data.get("success"):
                errors = data.get("errors", [])
                raise RuntimeError(f"Failed to fetch users (page {page}): {errors}")

            result = data.get("result")
            if result is None:
                raise RuntimeError(
                    f"Unexpected API response — 'result' is None. "
                    f"Check your credentials and account ID."
                )

            users.extend(result)
            info = data.get("result_info", {})
            total_pages = info.get("total_pages", 1)
            total_count = info.get("total_count", len(users))

            print(
                f"  Fetched page {page}/{total_pages} "
                f"({len(users)}/{total_count} users)",
                end="\r",
            )

            if page >= total_pages:
                break
            page += 1

        print()  # newline after \r progress
        return users

    # ── Session inspection ────────────────────────────────────────────────

    async def get_active_sessions(
        self, session: aiohttp.ClientSession, user_id: str
    ) -> List[Dict]:
        data = await self.get(
            session, f"access/users/{user_id}/active_sessions"
        )
        if not data.get("success"):
            return []
        return data.get("result") or []

    # ── Mutations ─────────────────────────────────────────────────────────

    async def delete_user(
        self, session: aiohttp.ClientSession, user_id: str
    ) -> Tuple[bool, str]:
        """Delete user — also revokes seats and tokens."""
        data = await self.delete(session, f"access/users/{user_id}")
        if data.get("success"):
            return True, "ok"
        errors = data.get("errors", [{"message": "unknown"}])
        return False, errors[0].get("message", str(errors))

    async def update_seat(
        self,
        session: aiohttp.ClientSession,
        user_id: str,
        access_seat: bool,
        gateway_seat: bool,
    ) -> Tuple[bool, str]:
        """Update seat assignment for a user."""
        payload = [
            {
                "seat_uid": user_id,
                "access_seat": access_seat,
                "gateway_seat": gateway_seat,
            }
        ]
        data = await self.patch(session, "access/seats", payload)
        if data.get("success"):
            return True, "ok"
        errors = data.get("errors", [{"message": "unknown"}])
        return False, errors[0].get("message", str(errors))


# ── Enrichment ────────────────────────────────────────────────────────────────

def format_ts(ts) -> str:
    """Convert unix timestamp or ISO string to readable format."""
    if ts is None:
        return "N/A"
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    if isinstance(ts, str) and ts:
        return ts
    return "N/A"


async def enrich_user(
    client: CloudflareClient,
    session: aiohttp.ClientSession,
    raw: Dict,
) -> UserRecord:
    """Build a UserRecord with session and app details."""
    rec = UserRecord(
        id=raw["id"],
        name=raw.get("name") or raw.get("email", "unknown"),
        email=raw.get("email", "unknown"),
        access_seat=raw.get("access_seat", False),
        gateway_seat=raw.get("gateway_seat", False),
        created_at=raw.get("created_at", ""),
        last_successful_login=raw.get("last_successful_login", ""),
    )

    sessions = await client.get_active_sessions(session, rec.id)
    rec.active_sessions = len(sessions)

    if sessions:
        first = sessions[0]
        rec.session_expiration = format_ts(first.get("expiration"))
        metadata = first.get("metadata") or {}
        rec.last_issued = format_ts(metadata.get("iat"))

        apps_map = metadata.get("apps") or {}
        for app_data in apps_map.values():
            rec.apps.append(
                AppInfo(
                    name=app_data.get("name", "N/A"),
                    hostname=app_data.get("hostname", "N/A"),
                    app_type=app_data.get("type", "N/A"),
                    uid=app_data.get("uid", "N/A"),
                )
            )

    return rec


# ── Reports ───────────────────────────────────────────────────────────────────

def print_summary(records: List[UserRecord]):
    total = len(records)
    active = sum(1 for r in records if r.active_sessions > 0)
    inactive = total - active
    seated_access = sum(1 for r in records if r.access_seat)
    seated_gw = sum(1 for r in records if r.gateway_seat)
    total_sessions = sum(r.active_sessions for r in records)
    total_apps = sum(len(r.apps) for r in records)

    print("\n╔══════════════════════════════════════════╗")
    print("║        ZERO TRUST ACCOUNT SUMMARY        ║")
    print("╠══════════════════════════════════════════╣")
    print(f"║  Total users          {total:>6}             ║")
    print(f"║  With active sessions {active:>6}             ║")
    print(f"║  No active sessions   {inactive:>6}             ║")
    print(f"║  Access seats used    {seated_access:>6}             ║")
    print(f"║  Gateway seats used   {seated_gw:>6}             ║")
    print(f"║  Total sessions       {total_sessions:>6}             ║")
    print(f"║  Total apps           {total_apps:>6}             ║")
    print("╚══════════════════════════════════════════╝")

    print(f"\n{'Name':<30} {'Email':<35} {'Sessions':>8} {'Access':>7} {'GW':>4} {'Apps':>5}")
    print("─" * 95)
    for r in sorted(records, key=lambda x: x.active_sessions, reverse=True):
        seat_a = "✓" if r.access_seat else "·"
        seat_g = "✓" if r.gateway_seat else "·"
        print(
            f"{r.name[:29]:<30} {r.email[:34]:<35} "
            f"{r.active_sessions:>8} {seat_a:>7} {seat_g:>4} {len(r.apps):>5}"
        )
    print("─" * 95)


def export_json(records: List[UserRecord], path: str):
    data = []
    for r in records:
        d = asdict(r)
        d["apps"] = [asdict(a) for a in r.apps]
        data.append(d)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  → JSON: {path}")


def export_csv_detail(records: List[UserRecord], path: str):
    fieldnames = [
        "Name", "Email", "Active Sessions", "Access Seat", "Gateway Seat",
        "Session Expiration", "Last Issued", "Created At",
        "Last Successful Login", "App Name", "App Hostname",
        "App Type", "App UID",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            base = {
                "Name": r.name,
                "Email": r.email,
                "Active Sessions": r.active_sessions,
                "Access Seat": r.access_seat,
                "Gateway Seat": r.gateway_seat,
                "Session Expiration": r.session_expiration,
                "Last Issued": r.last_issued,
                "Created At": r.created_at,
                "Last Successful Login": r.last_successful_login,
            }
            if r.apps:
                for app in r.apps:
                    row = {**base, "App Name": app.name, "App Hostname": app.hostname,
                           "App Type": app.app_type, "App UID": app.uid}
                    writer.writerow(row)
            else:
                writer.writerow(base)
    print(f"  → CSV (detail): {path}")


def export_csv_summary(records: List[UserRecord], path: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Email", "Active Sessions", "Access Seat",
                         "Gateway Seat", "Apps Count", "Last Login"])
        for r in sorted(records, key=lambda x: x.email):
            writer.writerow([
                r.name, r.email, r.active_sessions,
                r.access_seat, r.gateway_seat,
                len(r.apps), r.last_successful_login,
            ])
    print(f"  → CSV (summary): {path}")


def export_chart(records: List[UserRecord], path: str):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  ⚠ matplotlib not installed — skipping chart")
        return

    names = [r.name[:20] for r in records]
    sessions = [r.active_sessions for r in records]
    app_counts = [len(r.apps) for r in records]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(max(10, len(names) * 0.6), 10))

    bars1 = ax1.bar(names, sessions, color="#4A90D9")
    ax1.set_title("Active Sessions per User")
    ax1.set_ylabel("Sessions")
    ax1.tick_params(axis="x", rotation=45)
    for bar, val in zip(bars1, sessions):
        if val > 0:
            ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                     str(val), ha="center", va="bottom", fontsize=8)

    bars2 = ax2.bar(names, app_counts, color="#E5A84B")
    ax2.set_title("Apps per User")
    ax2.set_ylabel("Apps")
    ax2.tick_params(axis="x", rotation=45)
    for bar, val in zip(bars2, app_counts):
        if val > 0:
            ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                     str(val), ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  → Chart: {path}")


def export_all(records: List[UserRecord]):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("\nExporting reports...")
    export_json(records, f"zt_users_{ts}.json")
    export_csv_detail(records, f"zt_users_detail_{ts}.csv")
    export_csv_summary(records, f"zt_users_summary_{ts}.csv")
    export_chart(records, f"zt_users_chart_{ts}.png")


# ── Bulk operations ───────────────────────────────────────────────────────────

async def bulk_delete_users(
    client: CloudflareClient,
    session: aiohttp.ClientSession,
    targets: List[UserRecord],
    dry_run: bool = False,
):
    """Delete users, freeing seats and revoking tokens."""
    if not targets:
        print("  No users to delete.")
        return

    print(f"\n  Users targeted for deletion ({len(targets)}):")
    for r in targets:
        seat_info = []
        if r.access_seat:
            seat_info.append("access")
        if r.gateway_seat:
            seat_info.append("gateway")
        seat_str = f" [seats: {', '.join(seat_info)}]" if seat_info else ""
        print(f"    • {r.email} — {r.active_sessions} sessions{seat_str}")

    if dry_run:
        print("\n  🏷  DRY RUN — no changes made.")
        return

    confirm = input(f"\n  Delete these {len(targets)} users? Type 'yes' to confirm: ").strip()
    if confirm.lower() != "yes":
        print("  Cancelled.")
        return

    print("  Deleting...")
    ok, fail = 0, 0
    tasks = [client.delete_user(session, r.id) for r in targets]
    results = await asyncio.gather(*tasks)
    for rec, (success, msg) in zip(targets, results):
        if success:
            ok += 1
            print(f"    ✓ {rec.email}")
        else:
            fail += 1
            print(f"    ✗ {rec.email} — {msg}")

    print(f"\n  Done: {ok} deleted, {fail} failed")


async def bulk_remove_seats(
    client: CloudflareClient,
    session: aiohttp.ClientSession,
    targets: List[UserRecord],
    dry_run: bool = False,
):
    """Remove seat assignments without deleting user records."""
    if not targets:
        print("  No users with active seats.")
        return

    seated = [r for r in targets if r.access_seat or r.gateway_seat]
    if not seated:
        print("  No users with active seats in the target set.")
        return

    print(f"\n  Seats to remove ({len(seated)}):")
    for r in seated:
        parts = []
        if r.access_seat:
            parts.append("access")
        if r.gateway_seat:
            parts.append("gateway")
        print(f"    • {r.email} — removing: {', '.join(parts)}")

    if dry_run:
        print("\n  🏷  DRY RUN — no changes made.")
        return

    confirm = input(f"\n  Remove seats for {len(seated)} users? Type 'yes' to confirm: ").strip()
    if confirm.lower() != "yes":
        print("  Cancelled.")
        return

    print("  Updating seats...")
    ok, fail = 0, 0
    tasks = [
        client.update_seat(session, r.id, access_seat=False, gateway_seat=False)
        for r in seated
    ]
    results = await asyncio.gather(*tasks)
    for rec, (success, msg) in zip(seated, results):
        if success:
            ok += 1
            print(f"    ✓ {rec.email}")
        else:
            fail += 1
            print(f"    ✗ {rec.email} — {msg}")

    print(f"\n  Done: {ok} updated, {fail} failed")


# ── Interactive menu ──────────────────────────────────────────────────────────

def select_targets(records: List[UserRecord]) -> Tuple[List[UserRecord], bool]:
    """Let the user pick which users to act on and whether it's a dry run."""
    print("\n  Target selection:")
    print("    1) All users")
    print("    2) Only inactive (no active sessions)")
    print("    3) Only active (has active sessions)")
    print("    4) Only seated users (access or gateway seat)")
    print("    5) Pick by email (comma-separated)")

    choice = input("  Choice [1-5]: ").strip()

    if choice == "1":
        targets = records
    elif choice == "2":
        targets = [r for r in records if r.active_sessions == 0]
    elif choice == "3":
        targets = [r for r in records if r.active_sessions > 0]
    elif choice == "4":
        targets = [r for r in records if r.access_seat or r.gateway_seat]
    elif choice == "5":
        emails_raw = input("  Emails (comma-separated): ").strip()
        emails = {e.strip().lower() for e in emails_raw.split(",") if e.strip()}
        targets = [r for r in records if r.email.lower() in emails]
        missing = emails - {r.email.lower() for r in targets}
        if missing:
            print(f"  ⚠ Not found: {', '.join(missing)}")
    else:
        print("  Invalid choice.")
        return [], False

    dry = input("  Dry run? (y/n) [y]: ").strip().lower()
    dry_run = dry != "n"

    return targets, dry_run


async def interactive_loop(
    client: CloudflareClient, http: aiohttp.ClientSession, records: List[UserRecord]
):
    while True:
        print("\n┌──────────────────────────────────────┐")
        print("│     ZERO TRUST MANAGEMENT MENU       │")
        print("├──────────────────────────────────────┤")
        print("│  1) View summary                     │")
        print("│  2) View detailed user info           │")
        print("│  3) Export reports (JSON/CSV/chart)   │")
        print("│  4) Delete users (frees seats+tokens) │")
        print("│  5) Remove seats only (keep records)  │")
        print("│  6) Refresh data from API             │")
        print("│  7) Quit                              │")
        print("└──────────────────────────────────────┘")

        choice = input("\nChoice [1-7]: ").strip()

        if choice == "1":
            print_summary(records)

        elif choice == "2":
            for r in records:
                print(f"\n{'═' * 60}")
                print(f"  {r.name} <{r.email}>")
                print(f"  ID:             {r.id}")
                print(f"  Access seat:    {'✓' if r.access_seat else '✗'}")
                print(f"  Gateway seat:   {'✓' if r.gateway_seat else '✗'}")
                print(f"  Sessions:       {r.active_sessions}")
                print(f"  Expiration:     {r.session_expiration}")
                print(f"  Last issued:    {r.last_issued}")
                print(f"  Created:        {r.created_at}")
                print(f"  Last login:     {r.last_successful_login}")
                if r.apps:
                    print(f"  Apps ({len(r.apps)}):")
                    for a in r.apps:
                        print(f"    • {a.name} ({a.hostname}) [{a.app_type}]")
            print(f"{'═' * 60}")

        elif choice == "3":
            export_all(records)

        elif choice == "4":
            targets, dry_run = select_targets(records)
            if targets:
                await bulk_delete_users(client, http, targets, dry_run=dry_run)
                if not dry_run:
                    print("\n  ℹ  Run option 6 to refresh data after changes.")

        elif choice == "5":
            targets, dry_run = select_targets(records)
            if targets:
                await bulk_remove_seats(client, http, targets, dry_run=dry_run)
                if not dry_run:
                    print("\n  ℹ  Run option 6 to refresh data after changes.")

        elif choice == "6":
            print("\nRefreshing...")
            records = await fetch_and_enrich(client, http)
            print_summary(records)

        elif choice == "7":
            print("Bye.")
            break

        else:
            print("Invalid choice.")


# ── Bootstrap ─────────────────────────────────────────────────────────────────

async def fetch_and_enrich(
    client: CloudflareClient, http: aiohttp.ClientSession
) -> List[UserRecord]:
    print("Fetching users...")
    raw_users = await client.get_all_users(http)
    print(f"Enriching {len(raw_users)} users with session data...")
    tasks = [enrich_user(client, http, u) for u in raw_users]
    records = await asyncio.gather(*tasks)
    return list(records)


async def main():
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    api_key = os.environ.get("CLOUDFLARE_API_KEY")
    email = os.environ.get("CLOUDFLARE_EMAIL")
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")

    if not api_token and not (api_key and email):
        print(
            "Error: Set CLOUDFLARE_API_TOKEN (bearer) or both "
            "CLOUDFLARE_API_KEY + CLOUDFLARE_EMAIL (legacy)."
        )
        sys.exit(1)

    if not account_id:
        account_id = input("Enter your Cloudflare account ID: ").strip()

    client = CloudflareClient(
        account_id, api_token=api_token, api_key=api_key, email=email
    )
    print(f"Authenticated via {client.auth_mode}")

    async with aiohttp.ClientSession() as http:
        records = await fetch_and_enrich(client, http)
        print_summary(records)
        await interactive_loop(client, http, records)


if __name__ == "__main__":
    asyncio.run(main())
