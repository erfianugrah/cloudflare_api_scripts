"""
Async Cloudflare API client with unified auth, rate limiting, retries, and pagination.

Replaces the duplicated auth setup and zone-pagination logic found across 20+ scripts.
"""

import os
import time
import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Optional, Any, Callable

BASE_URL = "https://api.cloudflare.com/client/v4"


@dataclass
class AuthConfig:
    """Cloudflare API authentication configuration."""

    token: Optional[str] = None
    email: Optional[str] = None
    api_key: Optional[str] = None

    @classmethod
    def from_env(cls) -> "AuthConfig":
        """Build auth from environment variables."""
        return cls(
            token=_clean(os.environ.get("CLOUDFLARE_API_TOKEN", "")),
            email=_clean(os.environ.get("CLOUDFLARE_EMAIL", "")),
            api_key=_clean(os.environ.get("CLOUDFLARE_API_KEY", "")),
        )

    @classmethod
    def from_args_or_env(
        cls,
        token: str | None = None,
        email: str | None = None,
        api_key: str | None = None,
    ) -> "AuthConfig":
        """Build auth from CLI args with env-var fallback."""
        return cls(
            token=_clean(token or os.environ.get("CLOUDFLARE_API_TOKEN", "")),
            email=_clean(email or os.environ.get("CLOUDFLARE_EMAIL", "")),
            api_key=_clean(api_key or os.environ.get("CLOUDFLARE_API_KEY", "")),
        )

    @property
    def headers(self) -> dict[str, str]:
        if self.token:
            return {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }
        if self.email and self.api_key:
            return {
                "X-Auth-Email": self.email,
                "X-Auth-Key": self.api_key,
                "Content-Type": "application/json",
            }
        raise ValueError(
            "No valid auth credentials. Set CLOUDFLARE_API_TOKEN or "
            "CLOUDFLARE_EMAIL + CLOUDFLARE_API_KEY."
        )

    @property
    def mode(self) -> str:
        if self.token:
            return "Bearer Token"
        if self.email and self.api_key:
            return "API Key"
        return "None"

    def validate(self) -> None:
        _ = self.headers  # raises ValueError if missing


def _clean(val: str | None) -> str | None:
    """Strip whitespace and surrounding quotes; return None if empty."""
    if not val:
        return None
    val = val.strip().strip("\"'")
    return val or None


class CloudflareClient:
    """Async Cloudflare API client with rate limiting, retries, and pagination.

    Usage::

        auth = AuthConfig.from_env()
        async with CloudflareClient(auth) as client:
            zones = await client.get_all_zones()
    """

    def __init__(
        self,
        auth: AuthConfig | None = None,
        concurrency: int = 10,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        self.auth = auth or AuthConfig.from_env()
        self.auth.validate()
        self.concurrency = concurrency
        self.max_retries = max_retries
        self.timeout = timeout
        self._sem = asyncio.Semaphore(concurrency)
        self._session: aiohttp.ClientSession | None = None

        # Adaptive rate-limit state (updated from response headers)
        self._rate_remaining: int | None = None
        self._rate_reset: float | None = None  # monotonic timestamp
        self._rate_quota: int = 1200  # default; updated from Ratelimit-Policy
        self._rate_lock = asyncio.Lock()

    async def __aenter__(self) -> "CloudflareClient":
        self._session = aiohttp.ClientSession(
            headers=self.auth.headers,
            timeout=aiohttp.ClientTimeout(total=self.timeout),
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._session:
            await self._session.close()

    # ── Adaptive rate-limit tracking ──────────────────────────────────────
    #
    # Cloudflare uses RFC 9110 structured headers:
    #   Ratelimit:        "default";r=9998;t=1
    #   Ratelimit-Policy: "default";q=9999;w=300
    #
    # r = remaining requests, t = seconds until window resets
    # q = total quota,        w = window size in seconds

    @staticmethod
    def _parse_structured_rl(header: str | None) -> dict[str, int]:
        """Parse key=value pairs from a structured rate-limit header."""
        result: dict[str, int] = {}
        if not header:
            return result
        for part in header.split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                try:
                    result[k.strip()] = int(v.strip())
                except ValueError:
                    pass
        return result

    def _update_rate_limits(self, headers: Any) -> None:
        """Read Ratelimit / Ratelimit-Policy headers from every response."""
        rl = self._parse_structured_rl(headers.get("Ratelimit"))
        policy = self._parse_structured_rl(headers.get("Ratelimit-Policy"))

        if "r" in rl:
            self._rate_remaining = rl["r"]
        if "t" in rl:
            self._rate_reset = time.monotonic() + rl["t"]

        # Store the quota so we can calculate a meaningful 10% threshold
        if "q" in policy:
            self._rate_quota = policy["q"]

    async def _throttle_if_needed(self) -> None:
        """Pre-request backpressure when the rate-limit budget is running low.

        When remaining drops below 10% of the quota, evenly spread the
        remaining requests across the time left until the window resets.
        """
        async with self._rate_lock:
            if self._rate_remaining is None or self._rate_reset is None:
                return

            quota = getattr(self, "_rate_quota", 1200)
            threshold = max(int(quota * 0.10), 50)

            if self._rate_remaining > threshold:
                return  # plenty of budget

            now = time.monotonic()
            secs_left = max(self._rate_reset - now, 0.1)

            if self._rate_remaining <= 0:
                print(f"  Rate budget exhausted, waiting {secs_left:.1f}s for reset...")
                await asyncio.sleep(secs_left)
                return

            # Spread remaining requests evenly across time left
            delay = secs_left / self._rate_remaining
            if delay > 0.05:
                await asyncio.sleep(delay)

    # ── Core request ──────────────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        retries: int = 0,
        **kwargs: Any,
    ) -> dict:
        url = f"{BASE_URL}{path}" if not path.startswith("http") else path

        await self._throttle_if_needed()

        async with self._sem:
            async with self._session.request(method, url, **kwargs) as resp:
                # Always read rate-limit headers, even on errors
                self._update_rate_limits(resp.headers)

                if resp.status == 429:
                    wait = int(resp.headers.get("Retry-After", 5))
                    if retries < self.max_retries:
                        print(f"  Rate limited (429), waiting {wait}s...")
                        await asyncio.sleep(wait)
                        return await self._request(
                            method, path, retries + 1, **kwargs
                        )
                    raise RuntimeError(
                        f"Rate limited after {self.max_retries} retries"
                    )

                body = await resp.json()

                if resp.status >= 400:
                    errors = body.get("errors", [])
                    msg = (
                        errors[0].get("message", resp.reason)
                        if errors
                        else resp.reason
                    )
                    raise RuntimeError(f"API {resp.status}: {msg}")

                return body

    async def get(self, path: str, **kw: Any) -> dict:
        return await self._request("GET", path, **kw)

    async def post(self, path: str, **kw: Any) -> dict:
        return await self._request("POST", path, **kw)

    async def put(self, path: str, **kw: Any) -> dict:
        return await self._request("PUT", path, **kw)

    async def patch(self, path: str, **kw: Any) -> dict:
        return await self._request("PATCH", path, **kw)

    async def delete(self, path: str, **kw: Any) -> dict:
        return await self._request("DELETE", path, **kw)

    # ── Pagination ────────────────────────────────────────────────────────

    async def paginate(
        self,
        path: str,
        *,
        key: str = "result",
        params: dict | None = None,
        per_page: int = 100,
        on_page: Callable[[int, int], None] | None = None,
    ) -> list:
        """Fetch all pages of a paginated Cloudflare endpoint."""
        all_items: list = []
        page = 1
        p = dict(params or {})

        while True:
            p["page"] = page
            p["per_page"] = per_page
            data = await self.get(path, params=p)
            items = data.get(key, [])
            all_items.extend(items)

            if on_page:
                info = data.get("result_info", {})
                on_page(page, info.get("total_pages", page))

            info = data.get("result_info", {})
            total_pages = info.get("total_pages", 1)
            if page >= total_pages or not items:
                break
            page += 1

        return all_items

    # ── Convenience helpers ───────────────────────────────────────────────

    async def get_all_zones(
        self,
        on_page: Callable[[int, int], None] | None = None,
        **extra_params: Any,
    ) -> list[dict]:
        """Fetch every zone visible to this credential."""
        return await self.paginate(
            "/zones", per_page=1000, on_page=on_page, params=extra_params
        )

    async def get_all_zone_ids(self) -> list[str]:
        zones = await self.get_all_zones()
        return [z["id"] for z in zones]

    async def get_all_accounts(self) -> list[dict]:
        return await self.paginate("/accounts", per_page=50)
