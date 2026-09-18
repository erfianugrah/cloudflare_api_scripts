#!/usr/bin/env python3
"""Manage Cloudflare certificate packs for a zone (Advanced + Total TLS).

Commands:
  python3 delete_certs.py --zone erfi.io --list [--type total_tls]
      List cert packs. --type filters (advanced, total_tls, all).

  python3 delete_certs.py --zone erfi.io --select [--type X] [--dry-run]
      Interactive picker.

  python3 delete_certs.py --zone erfi.io [--type X] [--dry-run] < hosts.txt
      Batch delete hostnames from stdin.
"""

import argparse
import asyncio
import re
import sys
import os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from cf_lib.client import CloudflareClient, AuthConfig

_TYPE_DISPLAY = {"advanced": "Advanced", "total_tls": "Total TLS"}


def _build_pack_index(packs: list[dict], types: set[str]) -> list[dict]:
    """Return deduped, sorted list of cert pack entries matching given types."""
    rows = []
    seen_packs = set()
    for p in packs:
        ptype = p.get("type", "")
        if ptype not in types:
            continue
        pack_id = p["id"]
        if pack_id in seen_packs:
            continue
        seen_packs.add(pack_id)
        hosts = sorted(p.get("hosts", []))
        for h in hosts:
            rows.append({
                "host": h,
                "pack_id": pack_id,
                "type": ptype,
                "status": p.get("status", "?"),
                "expires": p.get("expires_on", "")[:10],
                "all_hosts": hosts,
            })
    # Singles first, then by type, then by hostname
    rows.sort(key=lambda r: (len(r["all_hosts"]) > 1, r["type"], r["host"]))
    return rows


def _pack_display(r: dict) -> str:
    """One-line display for a pack entry."""
    host = r["host"]
    if len(r["all_hosts"]) == 1:
        return host
    others = [h for h in r["all_hosts"] if h != host]
    return f"{host}  \033[90m[pack also covers: {', '.join(others)}]\033[0m"


# ── list ───────────────────────────────────────────────────────────────────

def cmd_list(rows: list[dict]):
    if not rows:
        print("No matching certificate packs found.")
        return
    print(f"{'TYPE':<12} {'STATUS':<20} {'EXPIRES':<12} HOSTNAME")
    print("-" * 80)
    for r in rows:
        status = r["status"]
        if status == "pending_validation":
            status = f"\033[33m{status}\033[0m"
        label = _TYPE_DISPLAY.get(r["type"], r["type"])
        print(f"{label:<12} {status:<28} {r['expires']:<12} {_pack_display(r)}")


# ── interactive picker ─────────────────────────────────────────────────────

def _parse_selection(raw: str, count: int) -> set[int]:
    raw = raw.strip().lower()
    if raw == "all":
        return set(range(1, count + 1))
    if raw == "none":
        return set()
    out: set[int] = set()
    for part in re.split(r"[\s,]+", raw):
        if not part:
            continue
        if "-" in part:
            try:
                a, b = part.split("-", 1)
                for i in range(int(a), int(b) + 1):
                    if 1 <= i <= count:
                        out.add(i)
            except ValueError:
                pass
        else:
            try:
                i = int(part)
                if 1 <= i <= count:
                    out.add(i)
            except ValueError:
                pass
    return out


async def cmd_select(rows: list[dict], dry_run: bool):
    if not rows:
        print("No certificate packs found (check --type).")
        return

    n = len(rows)
    selected: set[int] = set()
    last_input = ""

    done = False
    while not done:
        print(f"\n{'':>4} {'TYPE':<12} {'STATUS':<20} HOSTNAME")
        print(f"{'':>4} {'-'*60}")
        for i, r in enumerate(rows, 1):
            tick = "\033[32m[x]\033[0m" if i in selected else "[ ]"
            status = r["status"]
            if status == "pending_validation":
                status = f"\033[33m{status}\033[0m"
            label = _TYPE_DISPLAY.get(r["type"], r["type"])
            extra = ""
            if last_input and str(i) in last_input.replace(",", " ").split():
                extra = " \033[90m<--\033[0m"
            print(f"{tick} {i:>3} {label:<12} {status:<30} {_pack_display(r)}{extra}")

        try:
            w = os.get_terminal_size().columns
        except (OSError, ValueError):
            w = 80
        bar = f"  Selected {len(selected)}/{n}  |  numbers, ranges, all, none, q=confirm, c=cancel"
        if len(bar) > w:
            bar = bar[: w - 1]
        print(f"\033[90m{bar}\033[0m")
        cmd = input("> ").strip()

        if cmd.lower() == "q":
            if len(selected) == 0:
                print("Nothing selected. Bye.")
                return
            done = True
            continue
        if cmd.lower() == "c":
            print("Cancelled.")
            return

        if cmd:
            last_input = cmd
            new_sel = _parse_selection(cmd, n)
            if new_sel:
                selected = selected.symmetric_difference(new_sel)

    # Deduplicate by pack_id, collapse multi-host rows
    packs_to_delete: dict[str, list[str]] = {}
    for i in sorted(selected):
        r = rows[i - 1]
        pid = r["pack_id"]
        if pid not in packs_to_delete:
            packs_to_delete[pid] = r["all_hosts"]

    print(f"\n{'─'*50}")
    print(f"Deleting {len(packs_to_delete)} certificate packs:\n")
    for pid, hosts in packs_to_delete.items():
        label = ", ".join(hosts)
        print(f"  {label}")

    if dry_run:
        print("\n(dry-run -- no changes made)")
        return

    answer = input(f"\nProceed? [y/N]: ")
    if answer.lower() != "y":
        print("Aborted.")
        return

    await _delete_packs({pid: {"all_hosts": hosts} for pid, hosts in packs_to_delete.items()}, concurrency=10)


# ── batch stdin mode ───────────────────────────────────────────────────────

async def cmd_delete_all(rows: list[dict], dry_run: bool):
    """Delete every pack (deduplicated by pack_id)."""
    # Deduplicate by pack_id, collapse multi-host rows
    packs: dict[str, list[str]] = {}
    for r in rows:
        pid = r["pack_id"]
        if pid not in packs:
            packs[pid] = r["all_hosts"]

    if not packs:
        print("No matching certificate packs to delete.")
        return

    print(f"\n{'─'*50}")
    print(f"Deleting ALL {len(packs)} certificate packs:\n")
    for pid, hosts in packs.items():
        label = ", ".join(hosts)
        print(f"  {label}")

    if dry_run:
        print("\n(dry-run -- no changes made)")
        return

    answer = input(f"\nDelete all {len(packs)} packs? Type 'yes' to confirm: ")
    if answer.strip() != "yes":
        print("Aborted.")
        return

    await _delete_packs({pid: {"all_hosts": hosts} for pid, hosts in packs.items()}, concurrency=10)


def read_hostnames() -> list[str]:
    hostnames = []
    for line in sys.stdin:
        line = line.split("#")[0].strip()
        if line:
            hostnames.append(line)
    return hostnames


async def _delete_packs(packs: dict[str, dict], concurrency: int = 10):
    """Concurrently delete cert packs. packs = {pack_id: {all_hosts, type}}"""
    if not packs:
        return

    auth = AuthConfig.from_env()
    async with CloudflareClient(auth, concurrency=concurrency) as client:
        sem = asyncio.Semaphore(concurrency)
        ok = 0
        fail = 0

        async def delete_one(pid: str, info: dict):
            nonlocal ok, fail
            hosts = info["all_hosts"]
            label = hosts[0] if len(hosts) == 1 else ", ".join(hosts)
            async with sem:
                try:
                    await client.delete(f"/zones/{_zone_id}/ssl/certificate_packs/{pid}")
                    print(f"  \033[32mOK\033[0m   {label}")
                    ok += 1
                except Exception as e:
                    print(f"  \033[31mFAIL\033[0m {label}: {e}")
                    fail += 1

        await asyncio.gather(*[delete_one(pid, info) for pid, info in packs.items()])
        print(f"\nDone: {ok} deleted, {fail} failed.")


async def cmd_batch(zone_id: str, packs: list[dict], hostnames: list[str],
                    types: set[str], dry_run: bool, concurrency: int):
    host_to_pack: dict[str, dict] = {}
    for p in packs:
        if p.get("type", "") not in types:
            continue
        for h in p.get("hosts", []):
            host_to_pack[h] = {"id": p["id"], "all_hosts": sorted(p["hosts"]), "type": p["type"]}

    packs_to_delete: dict[str, dict] = {}
    not_found = []
    for h in hostnames:
        if h not in host_to_pack:
            not_found.append(h)
            continue
        info = host_to_pack[h]
        packs_to_delete[info["id"]] = info

    for h in not_found:
        print(f"NOT FOUND: {h}")
    for pid, info in packs_to_delete.items():
        hosts = info["all_hosts"]
        ptype = _TYPE_DISPLAY.get(info["type"], info["type"])
        if len(hosts) == 1:
            print(f"{'DRY-RUN:' if dry_run else 'DELETE':<10} {hosts[0]}  ({ptype})")
        else:
            print(f"{'DRY-RUN:' if dry_run else 'DELETE':<10} {', '.join(hosts)}  ({ptype})")

    if dry_run or not packs_to_delete:
        print(f"\n---\n{'Dry-run:' if dry_run else ''} {len(packs_to_delete)} packs, {len(not_found)} not found")
        return

    print(f"\nAbout to delete {len(packs_to_delete)} certificate packs.")
    if input("Proceed? [y/N]: ").lower() != "y":
        print("Aborted.")
        return

    await _delete_packs(packs_to_delete, concurrency=concurrency)


# ── main ───────────────────────────────────────────────────────────────────

_zone_id: str = ""


def _parse_types(arg: str) -> set[str]:
    """Parse --type flag: advanced|total_tls|all."""
    if not arg or arg == "all":
        return {"advanced", "total_tls"}
    parts = [p.strip() for p in arg.split(",")]
    return {p for p in parts if p in ("advanced", "total_tls")}


async def main():
    global _zone_id

    parser = argparse.ArgumentParser(description="Manage Cloudflare cert packs (Advanced + Total TLS)")
    parser.add_argument("--zone", required=True, help="Zone name (e.g. erfi.io)")
    parser.add_argument("--type", default="advanced", help="advanced, total_tls, all (default: advanced)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--list", action="store_true", help="List cert packs")
    group.add_argument("--select", action="store_true", help="Interactive picker")
    group.add_argument("--delete-all", action="store_true", help="Delete ALL matching cert packs")
    parser.add_argument("--dry-run", action="store_true", help="Show plan, do not delete")
    parser.add_argument("--concurrency", type=int, default=10, help="Parallel deletes (batch mode, default: 10)")
    args = parser.parse_args()

    types = _parse_types(args.type)
    type_labels = ", ".join(_TYPE_DISPLAY.get(t, t) for t in sorted(types))

    auth = AuthConfig.from_env()
    async with CloudflareClient(auth, concurrency=args.concurrency) as client:
        zones = await client.get(f"/zones?name={args.zone}")
        zone_list = zones.get("result", [])
        if not zone_list:
            print(f"ERROR: zone '{args.zone}' not found")
            sys.exit(1)
        _zone_id = zone_list[0]["id"]

        print(f"Zone: {args.zone}  Type: {type_labels}")
        print("Fetching certificate packs...")
        packs = await client.paginate(
            f"/zones/{_zone_id}/ssl/certificate_packs",
            params={"status": "all"},
            per_page=100,
        )

    rows = _build_pack_index(packs, types)

    if args.list:
        cmd_list(rows)
        return

    if args.select:
        await cmd_select(rows, args.dry_run)
        return

    if args.delete_all:
        await cmd_delete_all(rows, args.dry_run)
        return

    hostnames = read_hostnames()
    if not hostnames:
        print("No hostnames provided on stdin. Use --list or --select, or pipe hostnames.")
        sys.exit(1)

    print(f"Targets: {len(hostnames)} hostnames")
    for h in hostnames:
        print(f"  - {h}")
    await cmd_batch(_zone_id, packs, hostnames, types, args.dry_run, args.concurrency)


if __name__ == "__main__":
    asyncio.run(main())