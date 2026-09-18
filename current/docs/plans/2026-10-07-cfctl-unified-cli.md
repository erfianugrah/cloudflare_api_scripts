# cfctl: Unified Cloudflare Management CLI -- Implementation Plan

> **For agentic workers:** Execute this plan inline, task by task, in the current session. Steps use checkbox (`- [ ]`) syntax for tracking. Per-task subagents only if user asks.

**Goal:** Merge 4 cf_lib-based meta-CLIs + 1 R2 script into a single `cfctl` entrypoint with 5 namespaced command groups, archive 32 superseded scripts.

**Architecture:** Each domain module exports `register_commands(subparsers)` (registers its argparse subcommands on a given subparsers action) and an `execute(client, args)` async function. `cfctl` imports all four, builds the top-level parser with auth args, and dispatches. The `cert` domain (from `delete_certs.py`) merges into `cfctl_dns.py` as `cfctl dns cert {list,select,delete-all,batch}`.

**Key correction from v1 plan:** `cf_dns.py` and `cf_security.py` do NOT duplicate `_resolve_zones()` -- both delegate to `cf_lib.output.resolve_zones()` which handles `--zone-ids` (comma-separated IDs) but NOT `--zone` (name lookup). The cert commands need `--zone` name lookup. I'll extend `resolve_zones` in `cf_lib/output.py` to support `--zone` rather than creating a separate `ZoneResolver` module.

**Tech Stack:** Python 3, argparse, asyncio, `cf_lib` (CloudflareClient, AuthConfig, output helpers). No new dependencies.

**Files to read before starting:** All four source modules are read above and included in this plan verbatim at key points.

**Current state:**

| File | Lines | Module pattern | Zone arg |
|------|-------|----------------|----------|
| `cf_dns.py` | 424 | `build_parser()` + `run()` + `main()` + `COMMANDS` dict | `--zone-ids` via `resolve_zones()` |
| `cf_access.py` | 458 | Same pattern, no zone args | n/a (account-scoped) |
| `cf_security.py` | 523 | Same pattern | `--zone-ids` via `resolve_zones()` |
| `delete_certs.py` | 374 | Standalone `main()` with `_zone_id` global, mutual-exclusive mode flags | `--zone` (name) via inline API call |
| `async_list_r2_objects_per_bucket.py` | 84 | Raw aiohttp, interactive prompt | n/a (account-scoped) |
| `cf_lib/output.py` | ~130 | Shared: `resolve_zones()`, `add_auth_args()`, `print_table()`, etc. | `--zone-ids` only |

**Target state:**

```
cfctl                   <- dispatcher (new, ~60 lines)
cfctl_dns.py            <- cf_dns.py + cert commands from delete_certs.py
cfctl_access.py         <- cf_access.py + --markdown on members
cfctl_security.py       <- cf_security.py (unchanged except register_commands export)
cfctl_r2.py             <- R2 port (new, ~100 lines)
cf_lib/output.py        <- + --zone support in resolve_zones()
cf_ip_checker.py        <- unchanged (standalone)
legacy/                 <- 32 archived scripts
```

---## Task 1: Add `--zone` support to `cf_lib/output.py resolve_zones()`

**Why:** Both `cf_dns.py` and `cf_security.py` call `cf_lib.output.resolve_zones(client, args)` which handles `--zone-ids` (comma-separated zone IDs) but NOT `--zone` (name lookup). The cert commands in `delete_certs.py` need name lookup for `--zone erfi.io`. Instead of creating a separate `ZoneResolver` module, extend the existing shared function.

**Files:**
- Modify: `cf_lib/output.py` -- `resolve_zones()` function (lines 126-141)

**Current code** (lines 126-141 of `cf_lib/output.py`):

```python
async def resolve_zones(client, args, *, require_ids: bool = False) -> list[dict]:
    """Return zone dicts from --zone-ids flag, interactive prompt, or fetch-all.

    Shared by cf_dns.py and cf_security.py to avoid duplication.
    """
    zone_ids = getattr(args, "zone_ids", None)
    if zone_ids:
        ids = [zid.strip() for zid in zone_ids.split(",")]
        return [{"id": zid, "name": zid} for zid in ids]
    if require_ids:
        raw = input("Enter zone IDs (comma-separated): ").strip()
        if not raw:
            die("No zone IDs provided.")
        ids = [zid.strip() for zid in raw.split(",")]
        return [{"id": zid, "name": zid} for zid in ids]
    print("Fetching all zones...")
    return await client.get_all_zones()
```

- [ ] **Step 1: Replace `resolve_zones()` in `cf_lib/output.py`**

Replace the function body with:

```python
async def resolve_zones(client, args, *, require_ids: bool = False) -> list[dict]:
    """Return zone dicts from --zone-ids, --zone name lookup, interactive prompt, or fetch-all.

    Priority: --zone-ids > --zone name > interactive (if require_ids) > fetch all zones.
    Shared by cf_dns.py, cf_security.py, and cfctl cert commands.
    """
    # 1. Explicit zone IDs
    zone_ids = getattr(args, "zone_ids", None)
    if zone_ids:
        ids = [zid.strip() for zid in zone_ids.split(",")]
        return [{"id": zid, "name": zid} for zid in ids]

    # 2. Zone name lookup (used by cert commands: --zone erfi.io)
    zone_name = getattr(args, "zone", None)
    if zone_name:
        data = await client.get(f"/zones?name={zone_name}")
        result = data.get("result", [])
        if not result:
            raise SystemExit(f"Zone '{zone_name}' not found")
        return result

    # 3. Interactive prompt (only when require_ids=True, e.g. delete-records)
    if require_ids:
        raw = input("Enter zone IDs (comma-separated): ").strip()
        if not raw:
            die("No zone IDs provided.")
        ids = [zid.strip() for zid in raw.split(",")]
        return [{"id": zid, "name": zid} for zid in ids]

    # 4. Fetch all zones
    print("Fetching all zones...")
    return await client.get_all_zones()
```

- [ ] **Step 2: Verify existing behavior is preserved**

```
Run: python3 cf_dns.py zones
Expected: Same output as before (fetches all zones via path 4)

Run: python3 cf_dns.py records --zone-ids <valid-id>
Expected: Same output (uses path 1)
```

- [ ] **Step 3: Verify new --zone name lookup works**

This requires wiring `--zone` into a cf_dns subcommand first, which happens in Task 2. For now, test directly:

```
Run: python3 -c "
import asyncio, argparse
from cf_lib import CloudflareClient, AuthConfig
from cf_lib.output import resolve_zones

async def test():
    auth = AuthConfig.from_env()
    async with CloudflareClient(auth) as client:
        args = argparse.Namespace(zone_ids=None, zone='erfi.io')
        zones = await resolve_zones(client, args)
        print(f'Found: {zones[0][\"name\"]} ({zones[0][\"id\"]})')

asyncio.run(test())
"
Expected: Found: erfi.io (737f221be5119ebc5599cc4ef7a7a6dc)
```

- [ ] **Step 4: Commit**

```bash
git add cf_lib/output.py
git commit -m "feat: add --zone name lookup to resolve_zones()"
```

---

## Task 2: Merge cert pack commands into `cfctl_dns.py`

**Why:** `delete_certs.py` is a standalone script with its own parser, auth, zone resolution. Fold it under `cfctl dns cert` as a sub-subcommand group, reusing cf_dns's shared auth + client + zone resolution.

**Files:**
- Create: `cfctl_dns.py` (copy of `cf_dns.py`, then modified)
- After verification: archive `delete_certs.py` to `../legacy/`

**What changes in `cfctl_dns.py` vs `cf_dns.py`:**

1. Add `cert` subcommand with sub-subcommands: `list`, `select`, `delete-all`, `batch`
2. Add `--zone` arg to cert subcommands (for name lookup via `resolve_zones()`)
3. Copy handler functions from `delete_certs.py` with modifications (see below)
4. Remove old `cmd_certs` + `certs` subparser (the list-only non-active certs command)
5. Add `register_commands(subparsers)` export for the cfctl dispatcher

**Exact modifications to `cf_dns.py`:**

### 2a. Handler functions to add (before the `# -- CLI definition --` section)

Insert these functions after `cmd_spectrum` and before `# -- Helpers --`:

```python
# ── Cert pack lifecycle (merged from delete_certs.py) ────────────────────

_TYPE_DISPLAY = {"advanced": "Advanced", "total_tls": "Total TLS"}


def _build_cert_index(packs: list[dict], types: set[str]) -> list[dict]:
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
                "host": h, "pack_id": pack_id, "type": ptype,
                "status": p.get("status", "?"),
                "expires": p.get("expires_on", "")[:10],
                "all_hosts": hosts,
            })
    rows.sort(key=lambda r: (len(r["all_hosts"]) > 1, r["type"], r["host"]))
    return rows


def _pack_display(r: dict) -> str:
    host = r["host"]
    if len(r["all_hosts"]) == 1:
        return host
    others = [h for h in r["all_hosts"] if h != host]
    return f"{host}  \033[90m[pack also covers: {', '.join(others)}]\033[0m"


def _parse_cert_selection(raw: str, count: int) -> set[int]:
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


def _cert_list(rows: list[dict]):
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


def _cert_select(rows: list[dict], zone_id: str, dry_run: bool):
    import os as _os_mod
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
            w = _os_mod.get_terminal_size().columns
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
            new_sel = _parse_cert_selection(cmd, n)
            if new_sel:
                selected = selected.symmetric_difference(new_sel)

    # Deduplicate by pack_id
    packs_to_delete: dict[str, dict] = {}
    for i in sorted(selected):
        r = rows[i - 1]
        pid = r["pack_id"]
        if pid not in packs_to_delete:
            packs_to_delete[pid] = {"host": r["host"], "all_hosts": r["all_hosts"], "type": r["type"]}

    print(f"\n{'─'*50}")
    print(f"Deleting {len(packs_to_delete)} certificate packs:\n")
    for pid, info in packs_to_delete.items():
        hosts = info["all_hosts"]
        label = hosts[0] if len(hosts) == 1 else "\033[33m" + ", ".join(hosts) + "\033[0m"
        ptype = _TYPE_DISPLAY.get(info["type"], info["type"])
        print(f"  {label}  \033[90m({ptype})\033[0m")

    if dry_run:
        print("\n(dry-run -- no changes made)")
        return

    answer = input(f"\nProceed? [y/N]: ")
    if answer.lower() != "y":
        print("Aborted.")
        return

    # Must run async deletion from sync context
    asyncio.run(_cert_delete_packs(packs_to_delete, zone_id))


def _cert_delete_all(rows: list[dict], zone_id: str, dry_run: bool):
    """Delete every pack in rows (already filtered by type)."""
    packs: dict[str, dict] = {}
    for r in rows:
        pid = r["pack_id"]
        if pid not in packs:
            packs[pid] = r

    if not packs:
        print("No matching certificate packs to delete.")
        return

    print(f"\n{'─'*50}")
    print(f"Deleting ALL {len(packs)} certificate packs:\n")
    for pid, r in packs.items():
        hosts = r["all_hosts"]
        label = hosts[0] if len(hosts) == 1 else "\033[33m" + ", ".join(hosts) + "\033[0m"
        ptype = _TYPE_DISPLAY.get(r["type"], r["type"])
        print(f"  {label}  \033[90m({ptype})\033[0m")

    if dry_run:
        print("\n(dry-run -- no changes made)")
        return

    answer = input(f"\nDelete all {len(packs)} packs? Type 'yes' to confirm: ")
    if answer.strip() != "yes":
        print("Aborted.")
        return

    asyncio.run(_cert_delete_packs(packs, zone_id))


async def _cert_delete_packs(packs: dict[str, dict], zone_id: str, concurrency: int = 10):
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
                    await client.delete(f"/zones/{zone_id}/ssl/certificate_packs/{pid}")
                    print(f"  \033[32mOK\033[0m   {label}")
                    ok += 1
                except Exception as e:
                    print(f"  \033[31mFAIL\033[0m {label}: {e}")
                    fail += 1

        await asyncio.gather(*[delete_one(pid, info) for pid, info in packs.items()])
        print(f"\nDone: {ok} deleted, {fail} failed.")


def _parse_cert_types(arg: str) -> set[str]:
    if not arg or arg == "all":
        return {"advanced", "total_tls"}
    parts = [p.strip() for p in arg.split(",")]
    return {p for p in parts if p in ("advanced", "total_tls")}


async def _read_stdin_hostnames() -> list[str]:
    import sys as _sys_mod
    hostnames = []
    for line in _sys_mod.stdin:
        line = line.split("#")[0].strip()
        if line:
            hostnames.append(line)
    return hostnames


async def _cert_batch(client: CloudflareClient, zone_id: str, packs: list[dict],
                      hostnames: list[str], types: set[str],
                      dry_run: bool, concurrency: int):
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
    answer = input("Proceed? [y/N]: ")
    if answer.lower() != "y":
        print("Aborted.")
        return

    await _cert_delete_packs(packs_to_delete, zone_id, concurrency)


async def cmd_cert_dispatch(client: CloudflareClient, args: argparse.Namespace) -> None:
    """Dispatch cert sub-subcommands."""
    import sys as _sys_mod

    types = _parse_cert_types(getattr(args, "cert_type", "advanced"))

    # Resolve zone (uses --zone-ids or --zone from args)
    zones = await resolve_zones(client, args)
    if not zones:
        die("No zones found. Provide --zone or --zone-ids.")
    zone_id = zones[0]["id"]
    zone_name = zones[0].get("name", zone_id)

    print(f"Zone: {zone_name}  Types: {', '.join(_TYPE_DISPLAY.get(t, t) for t in sorted(types))}")
    print("Fetching certificate packs...")
    packs = await client.paginate(
        f"/zones/{zone_id}/ssl/certificate_packs",
        params={"status": "all"}, per_page=100,
    )

    rows = _build_cert_index(packs, types)
    action = args.cert_action

    if action == "list":
        _cert_list(rows)
    elif action == "select":
        dry = getattr(args, "dry_run", False)
        _cert_select(rows, zone_id, dry)
    elif action == "delete-all":
        dry = getattr(args, "dry_run", False)
        _cert_delete_all(rows, zone_id, dry)
    elif action == "batch":
        hostnames = await _read_stdin_hostnames()
        if not hostnames:
            die("No hostnames on stdin.")
        print(f"Targets: {len(hostnames)} hostnames")
        for h in hostnames:
            print(f"  - {h}")
        dry = getattr(args, "dry_run", False)
        await _cert_batch(client, zone_id, packs, hostnames, types, dry, 10)
```

### 2b. Modify COMMANDS dict and run()

Add to COMMANDS dict:

```python
COMMANDS = {
    "zones": cmd_zones,
    "records": cmd_records,
    "delete-records": cmd_delete_records,
    "hostnames": cmd_hostnames,
    "certs": cmd_certs,       # ← keep old (list non-active)
    "cert": cmd_cert_dispatch, # ← new (full lifecycle)
    "pending": cmd_pending,
    "dmarc": cmd_dmarc,
    "spectrum": cmd_spectrum,
}
```

### 2c. Modify build_parser() -- add cert subcommand group

Replace the old `certs` subparser with:

```python
    # OLD certs (non-active listing) -- keep
    p = sub.add_parser("certs", help="List non-active certificate packs")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--output", "-o", help="Write results to file (JSON)")

    # NEW cert (full lifecycle)
    cert_p = sub.add_parser("cert", help="Certificate pack lifecycle (Advanced + Total TLS)")
    cert_subs = cert_p.add_subparsers(dest="cert_action", required=True)

    c = cert_subs.add_parser("list", help="List cert packs")
    c.add_argument("--zone", help="Zone name (e.g. erfi.io)")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced",
                   help="advanced, total_tls, all (default: advanced)")

    c = cert_subs.add_parser("select", help="Interactive picker")
    c.add_argument("--zone", help="Zone name")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced")
    c.add_argument("--dry-run", action="store_true")

    c = cert_subs.add_parser("delete-all", help="Delete all matching cert packs")
    c.add_argument("--zone", help="Zone name")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced")
    c.add_argument("--dry-run", action="store_true")

    c = cert_subs.add_parser("batch", help="Batch delete hostnames from stdin")
    c.add_argument("--zone", help="Zone name")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced")
    c.add_argument("--dry-run", action="store_true")
```

### 2d. Add `register_commands()` export

At module level, add:

```python
def register_commands(subparsers):
    """Register dns subcommands on the given subparsers action (for cfctl dispatcher)."""
    dns_parser = subparsers.add_parser("dns", help="DNS and zone management")
    dns_subs = dns_parser.add_subparsers(dest="command", required=True)

    # ... copy all subparser setup from build_parser() here, but WITHOUT the top-level parser creation ...
```

Full code shown in Task 6.

- [ ] **Step 1: Create `cfctl_dns.py`**

```
Run: cp cf_dns.py cfctl_dns.py
```

- [ ] **Step 2: Apply all modifications above** (2a through 2d)

- [ ] **Step 3: Verify cert commands work through cfctl_dns.py**

```
Run: python3 cfctl_dns.py cert list --zone erfi.io --type all
Expected: Same output as delete_certs.py --zone erfi.io --list --type all

Run: python3 cfctl_dns.py cert delete-all --zone erfi.io --type total_tls --dry-run
Expected: Same output as delete_certs.py --zone erfi.io --delete-all --type total_tls --dry-run
```

- [ ] **Step 4: Verify old subcommands still work**

```
Run: python3 cfctl_dns.py zones
Run: python3 cfctl_dns.py records --proxied-only
Expected: Same output as cf_dns.py
```

- [ ] **Step 5: Archive delete_certs.py**

```
Run: mv delete_certs.py ../legacy/delete_certs.py
Verify: python3 cfctl_dns.py cert list --zone erfi.io
Expected: Still works (no import dependency on delete_certs.py)
```

- [ ] **Step 6: Commit**

```bash
git add cfctl_dns.py
git add ../legacy/delete_certs.py
git rm --cached delete_certs.py 2>/dev/null || true
git commit -m "feat: merge cert pack lifecycle into cfctl_dns cert subcommand"
```## Task 3: Add `--markdown` to `cfctl_access.py members`

**Why:** `list_members.py` exports account members to Markdown. `cf_access.py members` only exports CSV/JSON. Add `--markdown` so `list_members.py` can be archived.

**Files:**
- Create: `cfctl_access.py` (copy of `cf_access.py`)
- Modify: `members` subparser + `cmd_members` handler + `register_commands()` export

- [ ] **Step 1: Create `cfctl_access.py`**

```
Run: cp cf_access.py cfctl_access.py
```

- [ ] **Step 2: Add `--markdown` flag to members subparser**

In `build_parser()`, find the `members` subparser block (lines ~390 of cf_access.py):

```python
    # members
    p = sub.add_parser("members", help="List account members")
    p.add_argument("--output", "-o", help="Output file (.csv or .json)")
```

Replace with:

```python
    # members
    p = sub.add_parser("members", help="List account members")
    p.add_argument("--output", "-o", help="Output file (.csv or .json)")
    p.add_argument("--markdown", action="store_true", help="Export to Markdown file")
```

- [ ] **Step 3: Add Markdown export logic to `cmd_members`**

In `cmd_members()`, after the existing `print_table(rows, ...)` line for terminal output, insert BEFORE the return:

```python
    # Markdown export (before return, after all_rows is populated)
    if getattr(args, "markdown", False):
        md_path = timestamp_filename("members", ".md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("# Cloudflare Account Members\n\n")
            f.write(f"*Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n\n")
            f.write(f"| Account | Email | Status | Roles |\n")
            f.write(f"|---------|-------|--------|-------|\n")
            for r in all_rows:
                f.write(f"| {r['Account Name']} | {r['Member Email']} | {r['Status']} | {r['Roles']} |\n")
        print(f"\nWritten Markdown to {md_path}")
        return
```

- [ ] **Step 4: Verify**

```
Run: python3 cfctl_access.py members --markdown
Expected: Creates members_<timestamp>.md, prints path
```

- [ ] **Step 5: Add `register_commands()` export**

Same pattern as Task 2d. Full code in Task 6.

- [ ] **Step 6: Commit**

```bash
git add cfctl_access.py
git commit -m "feat: add --markdown export to access members command"
```

---

## Task 4: Port R2 bucket listing to `cfctl_r2.py`

**Why:** `async_list_r2_objects_per_bucket.py` uses raw aiohttp with hardcoded env-var auth. Port to cf_lib for consistent auth, CLI args, and output formatting.

**R2 API details** (verified from source):
- Endpoint: `GET /accounts/{account_id}/r2/buckets/{bucket_name}/objects?per_page=1000&delimiter=/`
- Cursor pagination: response has `"cursor"` key when more pages exist; pass `&cursor=<value>` to get next page
- Response shape: `{"success": true, "result": [{"key": "...", "size": N, "http_metadata": {...}}, ...]}`
- Objects are in `result` array directly, NOT nested under `objects`

**Files:**
- Create: `cfctl_r2.py`
- After verification: archive `async_list_r2_objects_per_bucket.py` to `../legacy/`

- [ ] **Step 1: Create `cfctl_r2.py`**

```python
#!/usr/bin/env python3
"""R2 bucket object listing.

Usage:
    python3 cfctl_r2.py list --account-id ACCT --bucket my-bucket
    python3 cfctl_r2.py list --account-id ACCT --bucket my-bucket --json
"""

import argparse
import asyncio
import sys

from cf_lib import CloudflareClient, AuthConfig
from cf_lib.output import add_auth_args, die, print_table, write_json


async def _r2_list(client: CloudflareClient, account_id: str, bucket: str):
    """Cursor-paginate through R2 objects. Returns (objects, total_size)."""
    objects = []
    total_size = 0
    cursor = None

    while True:
        url = f"/accounts/{account_id}/r2/buckets/{bucket}/objects"
        params = {"per_page": 1000, "delimiter": "/"}
        if cursor:
            params["cursor"] = cursor

        data = await client.get(url, params=params)
        result = data.get("result", [])
        for obj in result:
            objects.append({
                "key": obj["key"],
                "size": obj.get("size", 0),
                "content_type": obj.get("http_metadata", {}).get("contentType", "application/octet-stream"),
            })
            total_size += obj.get("size", 0)

        cursor = data.get("cursor")
        if not cursor or not result:
            break

        print(f"  Fetched {len(objects)} objects...", end="\r")

    return objects, total_size


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


async def cmd_list(client: CloudflareClient, args: argparse.Namespace) -> None:
    """List objects in an R2 bucket."""
    account_id = args.account_id
    bucket = args.bucket

    print(f"Bucket: {bucket}  (account {account_id})")
    print("Fetching objects...")
    objects, total_size = await _r2_list(client, account_id, bucket)

    print(f"\n{len(objects)} objects, {_fmt_bytes(total_size)} total\n")

    if args.json:
        write_json({
            "bucket": bucket,
            "total_count": len(objects),
            "total_size_bytes": total_size,
            "total_size": _fmt_bytes(total_size),
            "objects": objects,
        })
    else:
        rows = [(o["key"], _fmt_bytes(o["size"]), o["content_type"]) for o in objects]
        print_table(rows, ["Key", "Size", "Content Type"])


# ── CLI definition ────────────────────────────────────────────────────────


COMMANDS = {"list": cmd_list}


def register_commands(subparsers):
    """Register r2 subcommands on the given subparsers action."""
    r2_parser = subparsers.add_parser("r2", help="R2 bucket operations")
    r2_subs = r2_parser.add_subparsers(dest="r2_command", required=True)

    p = r2_subs.add_parser("list", help="List objects in a bucket")
    p.add_argument("--account-id", required=True, help="Cloudflare account ID")
    p.add_argument("--bucket", required=True, help="R2 bucket name")
    p.add_argument("--json", action="store_true", help="Output as JSON")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cfctl_r2", description="R2 bucket operations")
    add_auth_args(parser)
    sub = parser.add_subparsers(dest="r2_command", required=True)
    register_commands_for_standalone(sub)
    return parser


def register_commands_for_standalone(subparsers):
    """Same as register_commands but without wrapping in 'r2' group."""
    p = subparsers.add_parser("list", help="List objects in a bucket")
    p.add_argument("--account-id", required=True, help="Cloudflare account ID")
    p.add_argument("--bucket", required=True, help="R2 bucket name")
    p.add_argument("--json", action="store_true", help="Output as JSON")


async def run(args: argparse.Namespace) -> None:
    auth = AuthConfig.from_args_or_env(
        token=args.api_token, email=args.email, api_key=args.api_key,
    )
    print(f"Auth: {auth.mode}")
    handler = COMMANDS[args.r2_command]
    async with CloudflareClient(auth) as client:
        await handler(client, args)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.r2_command:
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
```

- [ ] **Step 2: Verify standalone mode**

```
Run: python3 cfctl_r2.py list --account-id <valid-id> --bucket <valid-bucket>
Expected: Lists objects with sizes, same data as async_list_r2_objects_per_bucket.py
```

- [ ] **Step 3: Archive old R2 script**

```
Run: mv async_list_r2_objects_per_bucket.py ../legacy/
```

- [ ] **Step 4: Commit**

```bash
git add cfctl_r2.py
git add ../legacy/async_list_r2_objects_per_bucket.py
git rm --cached async_list_r2_objects_per_bucket.py 2>/dev/null || true
git commit -m "feat: port R2 bucket listing to cfctl_r2 (cf_lib, cursor pagination)"
```

---

## Task 5: Rename `cf_security.py` -> `cfctl_security.py` (no code changes)

**Why:** Consistent naming. The security module has no functional changes needed.

**Files:**
- Create: `cfctl_security.py` (exact copy)
- Add: `register_commands()` export
- Archive: `cf_security.py` to `../legacy/`

- [ ] **Step 1: Copy and add register_commands()**

```
Run: cp cf_security.py cfctl_security.py
```

Add `register_commands()` at module level (exact code in Task 6).

- [ ] **Step 2: Verify standalone**

```
Run: python3 cfctl_security.py find-filters --expression "ip.src"
Expected: Same output as cf_security.py
```

- [ ] **Step 3: Archive**

```
Run: mv cf_security.py ../legacy/
```

- [ ] **Step 4: Commit**

```bash
git add cfctl_security.py ../legacy/cf_security.py
git rm --cached cf_security.py 2>/dev/null || true
git commit -m "refactor: rename cf_security -> cfctl_security for cfctl namespace"
```

---

## Task 6: Write the `cfctl` dispatcher

**Why:** Single entrypoint that delegates to all domain modules.

**Architecture:** Each domain module exports:
- `register_commands(subparsers)` -- registers its argparse subcommands on the given subparsers action, wrapping in a command group (e.g. `dns`, `access`, `security`, `r2`)
- `run(args)` -- wires auth, picks handler from its COMMANDS dict, creates client

The `cfctl` dispatcher calls all four `register_commands()`, parses, then calls the appropriate module's `run()`.

**Files:**
- Create: `cfctl`
- Modify: `cfctl_dns.py`, `cfctl_access.py`, `cfctl_security.py`, `cfctl_r2.py` -- add `register_commands()` export

### 6a. `register_commands()` for each module

**`cfctl_dns.py`:**

```python
def register_commands(subparsers):
    """Register dns subcommands on the given subparsers action."""
    dns_parser = subparsers.add_parser("dns", help="DNS and zone management")
    dns_subs = dns_parser.add_subparsers(dest="command", required=True)

    # zones
    p = dns_subs.add_parser("zones", help="List all zones")
    p.add_argument("--json", action="store_true", help="Output as JSON")

    # records
    p = dns_subs.add_parser("records", help="List DNS records")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--zone", help="Zone name (e.g. erfi.io)")
    p.add_argument("--proxied-only", action="store_true", help="Only proxied records")
    p.add_argument("--output", "-o", help="Write results to file (JSON)")

    # delete-records
    p = dns_subs.add_parser("delete-records", help="Delete all DNS records in zones")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs")

    # hostnames
    p = dns_subs.add_parser("hostnames", help="List custom hostnames")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--output", "-o", help="Write results to file (JSON)")

    # certs (list non-active only -- legacy)
    p = dns_subs.add_parser("certs", help="List non-active certificate packs")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--output", "-o", help="Write results to file (JSON)")

    # cert (full lifecycle)
    cert_p = dns_subs.add_parser("cert", help="Certificate pack lifecycle (Advanced + Total TLS)")
    cert_subs = cert_p.add_subparsers(dest="cert_action", required=True)

    c = cert_subs.add_parser("list", help="List cert packs")
    c.add_argument("--zone", help="Zone name (e.g. erfi.io)")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced")

    c = cert_subs.add_parser("select", help="Interactive picker")
    c.add_argument("--zone", help="Zone name")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced")
    c.add_argument("--dry-run", action="store_true")

    c = cert_subs.add_parser("delete-all", help="Delete all matching cert packs")
    c.add_argument("--zone", help="Zone name")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced")
    c.add_argument("--dry-run", action="store_true")

    c = cert_subs.add_parser("batch", help="Batch delete hostnames from stdin")
    c.add_argument("--zone", help="Zone name")
    c.add_argument("--zone-ids", help="Comma-separated zone IDs")
    c.add_argument("--type", dest="cert_type", default="advanced")
    c.add_argument("--dry-run", action="store_true")

    # pending
    p = dns_subs.add_parser("pending", help="Fetch pending zones with verification info")
    p.add_argument("--account", "-a", help="Single account ID to check")
    p.add_argument("--output", "-o", help="Output file (.csv or .json)")

    # dmarc
    p = dns_subs.add_parser("dmarc", help="Enable DMARC reporting for zones")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")

    # spectrum
    p = dns_subs.add_parser("spectrum", help="List or delete Spectrum apps")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed apps")
```

**`cfctl_access.py`:**

```python
def register_commands(subparsers):
    """Register access subcommands on the given subparsers action."""
    access_parser = subparsers.add_parser("access", help="Access and Zero Trust management")
    access_subs = access_parser.add_subparsers(dest="command", required=True)

    p = access_subs.add_parser("apps", help="List or delete Access applications")
    p.add_argument("--account-ids", help="Comma-separated account IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed apps")

    p = access_subs.add_parser("idps", help="List or delete Identity Providers")
    p.add_argument("--account-ids", help="Comma-separated account IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed IDPs")

    p = access_subs.add_parser("tokens", help="List or delete service tokens")
    p.add_argument("--account-ids", help="Comma-separated account IDs")
    p.add_argument("--delete", action="store_true", help="Delete the listed tokens")

    p = access_subs.add_parser("members", help="List account members")
    p.add_argument("--output", "-o", help="Output file (.csv or .json)")
    p.add_argument("--markdown", action="store_true", help="Export to Markdown file")

    p = access_subs.add_parser("users", help="Audit Zero Trust users and sessions")
    p.add_argument("--account-id", help="Account ID (or set CLOUDFLARE_ACCOUNT_ID)")
    p.add_argument("--output", "-o", help="Export to file (.csv or .json)")
    p.add_argument("--delete-inactive", action="store_true", help="Delete users with no active sessions")
    p.add_argument("--remove-seats", action="store_true", help="Clear seat assignments")
    p.add_argument("--inactive-only", action="store_true", help="With --remove-seats, only target inactive users")
    p.add_argument("--dry-run", action="store_true", help="Preview changes without applying")
```

**`cfctl_security.py`:**

```python
def register_commands(subparsers):
    """Register security subcommands on the given subparsers action."""
    security_parser = subparsers.add_parser("security", help="Security rules and rulesets")
    security_subs = security_parser.add_subparsers(dest="command", required=True)

    p = security_subs.add_parser("delete-fw-rules", help="Delete firewall rules")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--zone", help="Zone name (e.g. erfi.io)")
    p.add_argument("--description", help="Only delete rules matching this description (exact match)")
    p.add_argument("--fuzzy", action="store_true", help="Use substring match instead of exact for --description")

    p = security_subs.add_parser("find-filters", help="Find filters by expression")
    p.add_argument("--expression", required=True, help="Expression substring to search")
    p.add_argument("--output", "-o", help="Output file (JSON)")

    p = security_subs.add_parser("init-rulesets", help="Initialize empty custom rulesets")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--zone", help="Zone name")
    p.add_argument("--phase", default="http_request_firewall_custom", help="Phase name")

    p = security_subs.add_parser("clear-ruleset", help="Clear all rules in a ruleset phase")
    p.add_argument("--phase", required=True, help="Ruleset phase")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--zone", help="Zone name")

    p = security_subs.add_parser("purge-versions", help="Delete old ruleset versions")
    p.add_argument("--phase", required=True, help="Ruleset phase")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--zone", help="Zone name")

    p = security_subs.add_parser("revert", help="Revert a ruleset to a previous version")
    p.add_argument("--zone-id", help="Zone ID")
    p.add_argument("--phase", help="Ruleset phase (to auto-find ruleset)")
    p.add_argument("--source", help="Ruleset source filter (firewall_custom, firewall_managed)")
    p.add_argument("--ruleset-id", help="Specific ruleset ID")
    p.add_argument("--version", help="Specific version to revert to")

    p = security_subs.add_parser("migrate", help="Migrate firewall rules to custom rules")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--dry-run", action="store_true", help="Preview without applying")

    p = security_subs.add_parser("tf-import", help="Import rulesets into Terraform state")
    p.add_argument("--phase", required=True, help="Ruleset phase")
    p.add_argument("--zone-ids", help="Comma-separated zone IDs (default: all)")
    p.add_argument("--verbose", "-v", action="store_true", help="Show terraform output")
```

**`cfctl_r2.py`:**

```python
def register_commands(subparsers):
    """Register r2 subcommands on the given subparsers action."""
    r2_parser = subparsers.add_parser("r2", help="R2 bucket operations")
    r2_subs = r2_parser.add_subparsers(dest="r2_command", required=True)

    p = r2_subs.add_parser("list", help="List objects in a bucket")
    p.add_argument("--account-id", required=True, help="Cloudflare account ID")
    p.add_argument("--bucket", required=True, help="R2 bucket name")
    p.add_argument("--json", action="store_true", help="Output as JSON")
```

### 6b. `cfctl` dispatcher

```python
#!/usr/bin/env python3
"""cfctl -- Unified Cloudflare management CLI.

Usage:
    cfctl dns zones
    cfctl dns records --proxied-only
    cfctl dns cert list --zone erfi.io
    cfctl dns cert delete-all --zone erfi.io --type total_tls --dry-run
    cfctl access members --markdown
    cfctl security find-filters --expression "ip.src"
    cfctl r2 list --account-id ACCT --bucket my-bucket

Auth via environment variables (CLOUDFLARE_API_TOKEN, or CLOUDFLARE_EMAIL + CLOUDFLARE_API_KEY),
or --api-token / --api-key + --email CLI flags.
"""

import argparse
import asyncio
import sys

from cf_lib import AuthConfig
from cf_lib.output import add_auth_args, die

import cfctl_dns
import cfctl_access
import cfctl_security
import cfctl_r2


MODULES = {
    "dns": cfctl_dns,
    "access": cfctl_access,
    "security": cfctl_security,
    "r2": cfctl_r2,
}


async def main():
    parser = argparse.ArgumentParser(
        description="cfctl -- Unified Cloudflare management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_auth_args(parser)
    subparsers = parser.add_subparsers(dest="group", required=True, title="command groups")

    for mod in MODULES.values():
        mod.register_commands(subparsers)

    args = parser.parse_args()

    auth = AuthConfig.from_args_or_env(
        token=getattr(args, "api_token", None),
        email=getattr(args, "email", None),
        api_key=getattr(args, "api_key", None),
    )

    module = MODULES[args.group]
    try:
        await module.run(args)
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(130)
    except Exception as e:
        die(str(e))


if __name__ == "__main__":
    asyncio.run(main())
```

### 6c. Note on `run()` compatibility

The dispatcher calls `module.run(args)`. Each module's `run()` currently creates its own `AuthConfig` and `CloudflareClient`. This means auth is built TWICE (once in dispatcher for validation, once in module). This is acceptable -- the dispatcher's auth is just used to populate `args.api_token`/etc which the module then reads. Alternative: pass the client through. But the simpler approach works and doesn't require changing each module's `run()`.

**However** there's a subtlety: the dispatcher's `add_auth_args(parser)` registers `--api-token`, `--api-key`, `--email` on the TOP-LEVEL parser (not per-subcommand). This means `cfctl dns zones --api-token X` works, but `cfctl --api-token X dns zones` also works (argparse allows optional args before the subcommand). Each module's `run()` reads `args.api_token` etc which will exist regardless.

- [ ] **Step 1: Add `register_commands()` to cfctl_dns.py, cfctl_access.py, cfctl_security.py, cfctl_r2.py**

Apply the code from 6a to each file.

- [ ] **Step 2: Create `cfctl` dispatcher**

Apply the code from 6b.

- [ ] **Step 3: Make `cfctl` executable**

```
Run: chmod +x cfctl
```

- [ ] **Step 4: Verify `cfctl --help`**

```
Run: python3 cfctl --help
Expected: Shows dns, access, security, r2 command groups + auth args
```

- [ ] **Step 5: Smoke test each group**

```
Run: python3 cfctl dns zones
Run: python3 cfctl dns cert list --zone erfi.io --type all
Run: python3 cfctl access members
Run: python3 cfctl security find-filters --expression "ip.src"
Run: python3 cfctl r2 list --account-id <valid> --bucket <valid>
```

- [ ] **Step 6: Commit**

```bash
git add cfctl cfctl_dns.py cfctl_access.py cfctl_security.py cfctl_r2.py
git commit -m "feat: cfctl unified CLI dispatcher with 5 command groups"
```

---

## Task 7: Archive 32 superseded scripts to `legacy/`

**Why:** Dead code cleanup. All 27 legacy scripts + `cf_dns.py` + `cf_access.py` + `cf_security.py` + `delete_certs.py` + `async_list_r2_objects_per_bucket.py` + `list_members.py` + `async_zt_seat_manager.py` = 32 scripts.

Exact list (verified via `glob`):

```
delete_all_access_apps.py           delete_all_dns_records.py
delete_all_filters_by_expression.py delete_all_firewall_rules.py
delete_all_firewall_rules_with_the_same_description.py
delete_all_idps.py                  delete_all_rules_in_a_ruleset_all_zones.py
delete_all_rules_in_a_ruleset_one_zone.py
delete_all_service_tokens.py        delete_all_spectrum_apps.py
delete_rules_in_ruleset_via_version_id.py
enable_dmarc_all_zones.py           fetch_all_pending_zones_txt.py
intiate_custom_rulesets_for_zones.py
list_all_custom_hostnames.py        list_all_proxied_dns_records.py
list_all_zone_ids_in_a_list.py      list_non_active_certs.py
migrate_firewall_rules_to_custom_rules.py
revert_to_previous_version_for_custom_ruleset.py
revert_to_previous_version_for_managed_ruleset.py
revert_to_specific_version_for_custom_ruleset.py
bulk_import_tf_rulesets.py          async_dns_resolution.py
async_get_all_active_sessions.py    async_list_all_custom_hostnames.py
async_remove_inactive_users.py      list_members.py
async_zt_seat_manager.py            cf_dns.py
cf_access.py                        cf_security.py
delete_certs.py                     async_list_r2_objects_per_bucket.py
```

- [ ] **Step 1: Move all 32 scripts**

```bash
mkdir -p ../legacy
for f in delete_all_access_apps.py delete_all_dns_records.py \
  delete_all_filters_by_expression.py delete_all_firewall_rules.py \
  delete_all_firewall_rules_with_the_same_description.py \
  delete_all_idps.py delete_all_rules_in_a_ruleset_all_zones.py \
  delete_all_rules_in_a_ruleset_one_zone.py \
  delete_all_service_tokens.py delete_all_spectrum_apps.py \
  delete_rules_in_ruleset_via_version_id.py \
  enable_dmarc_all_zones.py fetch_all_pending_zones_txt.py \
  intiate_custom_rulesets_for_zones.py \
  list_all_custom_hostnames.py list_all_proxied_dns_records.py \
  list_all_zone_ids_in_a_list.py list_non_active_certs.py \
  migrate_firewall_rules_to_custom_rules.py \
  revert_to_previous_version_for_custom_ruleset.py \
  revert_to_previous_version_for_managed_ruleset.py \
  revert_to_specific_version_for_custom_ruleset.py \
  bulk_import_tf_rulesets.py async_dns_resolution.py \
  async_get_all_active_sessions.py async_list_all_custom_hostnames.py \
  async_remove_inactive_users.py list_members.py \
  async_zt_seat_manager.py cf_dns.py cf_access.py cf_security.py \
  delete_certs.py async_list_r2_objects_per_bucket.py; do
  [ -f "$f" ] && mv "$f" ../legacy/
done
```

- [ ] **Step 2: Create `legacy/README.md`**

```markdown
# Legacy scripts

Archived 2026-10-07 during cfctl migration. Superseded by:

- `cfctl dns` (zones, records, hostnames, certs, DMARC, Spectrum, pending zones)
- `cfctl access` (apps, IDPs, tokens, members, users)
- `cfctl security` (firewall rules, filters, rulesets, migration, TF import)
- `cfctl r2` (bucket object listing)

Remaining standalone scripts at `../current/`:
- `cf_ip_checker.py` -- Cloudflare IP range lookup (public data, no auth needed)
- `nmap.py` -- network scanning utility

These scripts remain for reference but are no longer maintained.
```

- [ ] **Step 3: Verify cfctl still works after moves**

```
Run: python3 cfctl dns zones
Run: python3 cfctl dns cert list --zone erfi.io
Expected: Both work (no import dependency on archived files)
```

- [ ] **Step 4: Final directory listing**

```
Run: ls *.py
Expected: cfctl, cfctl_dns.py, cfctl_access.py, cfctl_security.py, cfctl_r2.py, cf_ip_checker.py, nmap.py
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: archive 32 superseded scripts to legacy/, finalize cfctl migration"
```

---

## Task 8: [Optional] Convenience symlink

- [ ] **Step 1: Create symlink at `~/.local/bin/cfctl` (verified on PATH)**

```
Run: ln -sf ~/cf-stuff/cloudflare_api_scripts/current/cfctl ~/.local/bin/cfctl
```

- [ ] **Step 2: Verify**

```
Run: which cfctl
Expected: /home/erfi/.local/bin/cfctl

Run: cfctl --help
Expected: Help output
```

---

## Edge cases & gotchas

### `_zone_id` global removal

`delete_certs.py` uses a module-level `_zone_id` variable that `_delete_packs()` reads to build `/zones/{_zone_id}/ssl/certificate_packs/{pid}` URLs. In the merged `cfctl_dns.py`, this becomes an explicit parameter: `_cert_delete_packs(packs, zone_id)` and `zone_id` is resolved in `cmd_cert_dispatch()` from `resolve_zones()`.

### `_delete_packs()` called from sync context

`_cert_select()` and `_cert_delete_all()` are synchronous functions (they do terminal I/O). But they need to call `_cert_delete_packs()` which is async. They use `asyncio.run(_cert_delete_packs(...))` -- this creates a fresh event loop each time. This is fine because `cmd_cert_dispatch()` receives the client from the dispatcher's `run()`, but the cert deletion uses its own client (via `AuthConfig.from_env()`) inside `_cert_delete_packs`. This is intentionally isolated -- the main client's rate limiter shouldn't interfere with the cert delete batch.

### `cmd_certs` vs `cmd_cert_dispatch`

The old `certs` subcommand (list non-active certs only) is kept as `dns certs` (plural, no sub-subcommands). The new cert lifecycle is `dns cert` (singular, with sub-subcommands `list|select|delete-all|batch`). Both coexist -- `certs` is a simpler read-only listing across all zones.

### R2 cursor pagination

The R2 API uses `"cursor" in data` to signal more pages (NOT `result.get("truncated")`). The `per_page` parameter sets page size, `delimiter=/` is always passed. The cursor comes from the top-level response body, not nested under `result`.

### `build_parser()` vs `register_commands()`

Each module MUST keep `build_parser()` for standalone use (`python3 cfctl_dns.py zones`). `register_commands()` is the export for the cfctl dispatcher. The `build_parser()` in each module:
1. Creates its own top-level parser with `add_auth_args(parser)`
2. Adds subcommands directly (NOT wrapped in a group parser)
3. Returns the parser

This means each module can still run standalone for debugging.

### `run()` takes `args` with `api_token` attribute

The dispatcher's `add_auth_args()` puts `--api-token`/`--api-key`/`--email` on the top-level parser. Each module's `run()` reads `args.api_token` etc. This works because argparse allows optional args to appear before the subcommand. The `args` namespace from the top-level parser includes all top-level optional args, even when a subcommand is specified.

### Rate limiting during cert batch deletes

`_cert_delete_packs()` creates its own `CloudflareClient` with adaptive rate limiting. This is separate from the main client used by the dispatcher. For 50+ pack deletes, the adaptive throttle slows things down gracefully.

---

## Self-Review

**Placeholder scan:** Zero TBDs/TODOs/fill-in-laters. Every step has concrete code or commands.

**Type consistency:** `resolve_zones()` returns `list[dict]` with `id` and `name` keys throughout. `_cert_delete_packs()` takes `dict[str, dict]` with `all_hosts` key in the inner dict -- matches `_build_cert_index` output used by `_cert_select`, `_cert_delete_all`, and `_cert_batch`.

**Edge cases checked:** `_zone_id` global removal, sync-vs-async in cert select/delete-all, old `certs` vs new `cert` coexistence, R2 cursor pagination shape, standalone vs cfctl parser builds, auth arg propagation from top-level parser.

**All 9 entrypoints accounted for:** 4 merged into cfctl (dns/cert, access, security, r2), 1 kept standalone (cf_ip_checker), 4 archived (cf_dns, cf_access, cf_security, delete_certs). 32 total scripts archived.