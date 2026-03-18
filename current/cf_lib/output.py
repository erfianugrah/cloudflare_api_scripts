"""Shared output helpers: tables, JSON/CSV export, progress, confirmations."""

import csv
import json
import sys
from datetime import datetime
from typing import Any, Sequence


# ── Tables ────────────────────────────────────────────────────────────────────

def print_table(
    rows: Sequence[Sequence[Any]],
    headers: Sequence[str],
    *,
    max_col: int = 50,
) -> None:
    """Print an aligned ASCII table to stdout."""
    widths = [len(str(h)) for h in headers]
    str_rows = []
    for row in rows:
        cells = [str(c)[:max_col] for c in row]
        for i, c in enumerate(cells):
            if i < len(widths):
                widths[i] = max(widths[i], len(c))
        str_rows.append(cells)

    hdr = " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    sep = "-+-".join("-" * w for w in widths)
    print(hdr)
    print(sep)
    for row in str_rows:
        print(" | ".join(c.ljust(widths[i]) for i, c in enumerate(row)))


# ── JSON / CSV export ─────────────────────────────────────────────────────────

def write_json(data: Any, path: str | None = None) -> None:
    """Write data as formatted JSON to a file or stdout."""
    output = json.dumps(data, indent=2, default=str)
    if path:
        with open(path, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Written to {path}")
    else:
        print(output)


def write_csv(
    rows: list[dict],
    headers: list[str],
    path: str,
) -> None:
    """Write a list of dicts to CSV."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Written {len(rows)} records to {path}")


def timestamp_filename(prefix: str, ext: str) -> str:
    """Generate a timestamped filename like 'prefix_20260318_120000.ext'."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext}"


# ── Progress ──────────────────────────────────────────────────────────────────

def progress(current: int, total: int, prefix: str = "") -> None:
    """Display an inline progress bar."""
    width = 30
    filled = int(width * current / total) if total > 0 else 0
    bar = "#" * filled + "-" * (width - filled)
    pct = (current / total * 100) if total > 0 else 0
    print(f"\r{prefix}[{bar}] {current}/{total} ({pct:.0f}%)", end="", flush=True)
    if current >= total:
        print()


# ── Confirmation ──────────────────────────────────────────────────────────────

def confirm(message: str, default: bool = False) -> bool:
    """Prompt for yes/no confirmation."""
    suffix = " [Y/n]: " if default else " [y/N]: "
    try:
        resp = input(message + suffix).strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    if not resp:
        return default
    return resp in ("y", "yes")


# ── Common CLI bootstrap ─────────────────────────────────────────────────────

def add_auth_args(parser) -> None:
    """Add --api-token, --api-key, --email to an argparse parser."""
    auth = parser.add_argument_group("authentication")
    auth.add_argument("--api-token", help="Cloudflare API token (or set CLOUDFLARE_API_TOKEN)")
    auth.add_argument("--api-key", help="Cloudflare API key (or set CLOUDFLARE_API_KEY)")
    auth.add_argument("--email", help="Cloudflare email (or set CLOUDFLARE_EMAIL)")


def die(msg: str, code: int = 1) -> None:
    """Print error and exit."""
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(code)


# ── Zone / Account ID resolution ─────────────────────────────────────────────

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
