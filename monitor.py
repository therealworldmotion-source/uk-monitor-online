#!/usr/bin/env python3
"""
UK Pokemon TCG Retailer Monitor — Railway edition
─────────────────────────────────────────────────
Tracks specific Pokemon TCG products across:
  • Smyths Toys      (per-product, Slough store-stock + online)
  • Argos            (per-product, Slough store-stock + online)
  • Menkind          (whole Pokemon TCG category, in-stock alerts + cart permalinks)

Designed for a single Railway container. State persists to DATA_DIR (mount a 1 GB volume at /data).
All config via env vars — see README.md.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import random
import re
import time
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup
from patchright.async_api import BrowserContext, async_playwright

# ─── ENV CONFIG ───────────────────────────────────────────────────────────────

DATA_DIR = Path(os.environ.get("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# Comma-separated full product URLs. URL is the source of truth — IDs are extracted from the path.
SMYTHS_PRODUCT_URLS = [
    u.strip() for u in os.environ.get("SMYTHS_PRODUCT_URLS", "").split(",") if u.strip()
]
ARGOS_PRODUCT_URLS = [
    u.strip() for u in os.environ.get("ARGOS_PRODUCT_URLS", "").split(",") if u.strip()
]

# Slough store coordinates — used by Smyths store-pickup API to surface the Slough store first.
STORE_POSTCODE = os.environ.get("STORE_POSTCODE", "SL2 1EX")
STORE_LAT = os.environ.get("STORE_LAT", "51.510665")
STORE_LNG = os.environ.get("STORE_LNG", "-0.59888")
STORE_NAME_SMYTHS = os.environ.get("STORE_NAME_SMYTHS", "slough")  # case-insensitive match
STORE_NAME_ARGOS = os.environ.get("STORE_NAME_ARGOS", "slough")    # substring match

# Menkind category URL — Pokemon TCG search results.
MENKIND_URL = os.environ.get(
    "MENKIND_URL",
    "https://www.menkind.co.uk/search.php?BigCommerceX%5Bquery%5D=pokemon+TCG",
)

# John Lewis category / search URL (Pokemon TCG). Whole-category scrape, no per-product list needed.
JOHN_LEWIS_URL = os.environ.get(
    "JOHN_LEWIS_URL",
    "https://www.johnlewis.com/search?search-term=pokemon%20tcg",
)

# Very category URL (Pokemon TCG). The /search/... URL 403s, but /e/q/<term>.end works.
VERY_URL = os.environ.get(
    "VERY_URL",
    "https://www.very.co.uk/e/q/pokemon%20tcg.end",
)

# Check intervals (seconds). Defaults are sane — override per-retailer if needed.
INTERVAL_SMYTHS = int(os.environ.get("INTERVAL_SMYTHS", "180"))
INTERVAL_ARGOS = int(os.environ.get("INTERVAL_ARGOS", "180"))
INTERVAL_MENKIND = int(os.environ.get("INTERVAL_MENKIND", "180"))
INTERVAL_JOHN_LEWIS = int(os.environ.get("INTERVAL_JOHN_LEWIS", "180"))
INTERVAL_VERY = int(os.environ.get("INTERVAL_VERY", "180"))

# Imperva bypass — see resolve_smyths_ip() docstring
IMPERVA_DNS = os.environ.get("IMPERVA_DNS", "1.1.1.1")
SMYTHS_FORCE_IP = os.environ.get("SMYTHS_FORCE_IP", "")  # set to a known-good IP to skip resolution

# Optional proxy — set if Railway's IPs get bot-flagged by Imperva/Akamai.
# Format examples:
#   PROXY_URL=http://user:pass@gate.smartproxy.com:7000
#   PROXY_URL=http://username-rotate:password@p.webshare.io:80
# When set, all Chromium contexts route through the proxy and the Smyths host-resolver pin is skipped
# (the proxy handles DNS). Recommended providers: Webshare ($1.30/GB), IPRoyal ($1.75/GB), Smartproxy ($7/mo).
PROXY_URL = os.environ.get("PROXY_URL", "").strip()

# Disabled retailers (comma-separated keys)
DISABLED = {
    s.strip().lower() for s in os.environ.get("DISABLED_RETAILERS", "").split(",") if s.strip()
}

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)
# httpx logs every request at INFO — too noisy for our 30s long-poll cycle
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ─── BROWSER CONSTANTS ────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
]

MAX_TG_LENGTH = 4000

# Mutable heartbeat — monitor loop writes, watchdog reads
HEARTBEAT: dict[str, float] = {"last": 0.0}

# ─── STATE ────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            log.warning("State file corrupt, starting fresh")
    return {"smyths": {}, "argos": {}, "menkind": {}}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────

async def send_telegram(message: str, client: httpx.AsyncClient) -> int | None:
    if not TELEGRAM_ENABLED:
        log.info("[TG-DISABLED] %s", message[:160].replace("\n", " | "))
        return None
    chunks: list[str] = []
    while len(message) > MAX_TG_LENGTH:
        split_at = message.rfind("\n", 0, MAX_TG_LENGTH)
        if split_at == -1:
            split_at = MAX_TG_LENGTH
        chunks.append(message[:split_at])
        message = message[split_at:].lstrip("\n")
    chunks.append(message)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    first_id: int | None = None
    for chunk in chunks:
        try:
            resp = await client.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": chunk,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.status_code == 200:
                if first_id is None:
                    first_id = resp.json().get("result", {}).get("message_id")
            else:
                log.error("Telegram %s: %s", resp.status_code, resp.text[:200])
            await asyncio.sleep(0.4)
        except Exception as exc:
            log.error("Telegram send failed: %s", exc)
    return first_id


async def edit_telegram(message_id: int, message: str, client: httpx.AsyncClient) -> bool:
    if not TELEGRAM_ENABLED:
        return True
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    try:
        resp = await client.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "message_id": message_id,
                "text": message[:MAX_TG_LENGTH],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if resp.status_code == 400 and "not modified" in resp.text:
            return True
        return resp.status_code == 200
    except Exception as exc:
        log.error("Telegram edit failed: %s", exc)
        return False


async def poll_telegram(offset: int, client: httpx.AsyncClient) -> list[dict]:
    if not TELEGRAM_ENABLED:
        await asyncio.sleep(30)
        return []
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = await client.get(url, params={"offset": offset, "timeout": 30}, timeout=35)
        if resp.status_code == 200:
            return resp.json().get("result", [])
        # 401 (bad token), 404 (no bot found), etc — back off so we don't hot-loop
        log.warning("Telegram poll returned %s — backing off 30s", resp.status_code)
        await asyncio.sleep(30)
    except Exception as exc:
        log.error("Telegram poll error: %s", exc)
        await asyncio.sleep(5)
    return []


async def run_watchdog(monitor_task: asyncio.Task, client: httpx.AsyncClient) -> None:
    """Alerts if heartbeat goes stale or the monitor task crashes."""
    STALE = 600
    CHECK = 120
    alerted = False
    await asyncio.sleep(60)  # grace
    while not monitor_task.done():
        await asyncio.sleep(CHECK)
        age = time.monotonic() - HEARTBEAT["last"]
        if HEARTBEAT["last"] > 0 and age > STALE:
            if not alerted:
                await send_telegram(
                    "🚨 <b>UK Monitor not responding</b>\n\nNo heartbeat for 10+ min.\nSend <code>stop</code> then <code>start</code>.",
                    client,
                )
                alerted = True
        else:
            if alerted:
                await send_telegram("✅ <b>UK Monitor recovered.</b>", client)
            alerted = False
    if not monitor_task.cancelled():
        try:
            exc = monitor_task.exception()
            if exc:
                await send_telegram(
                    f"🚨 <b>UK Monitor crashed</b>\n\n<code>{type(exc).__name__}: {exc}</code>\n\nSend <code>start</code> to restart.",
                    client,
                )
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def product_key(s: str) -> str:
    return s.lower().strip().replace(" ", "-").replace("/", "-")[:80]


def smyths_id_from_url(url: str) -> str | None:
    m = re.search(r"/p/(\d+)", url)
    return m.group(1) if m else None


def argos_id_from_url(url: str) -> str | None:
    m = re.search(r"/product/(\d+)", url)
    return m.group(1) if m else None


# ─── IMPERVA DNS BYPASS ───────────────────────────────────────────────────────
# Smyths is behind Imperva, which appears to NXDOMAIN datacenter resolvers.
# Workaround: resolve via a public DNS (1.1.1.1) and pin the IP at Chromium launch
# via --host-resolver-rules. Falls back to no-op if resolution succeeds normally.

def resolve_smyths_ip() -> str | None:
    """Resolve www.smythstoys.com to an IP we can pin Chromium to.
    Pinning is REQUIRED — Imperva blocks Chromium's built-in DNS / direct connections
    even when OS DNS works fine. Confirmed by smoke test: without --host-resolver-rules
    the page hangs / times out; with it, returns 200.

    Order: SMYTHS_FORCE_IP override → OS resolver → public DNS (1.1.1.1) fallback."""
    if SMYTHS_FORCE_IP:
        log.info("Smyths: pinning SMYTHS_FORCE_IP=%s (override)", SMYTHS_FORCE_IP)
        return SMYTHS_FORCE_IP
    try:
        import socket
        ip = socket.gethostbyname("www.smythstoys.com")
        log.info("Smyths: OS resolver returned %s — pinning Chromium to it", ip)
        return ip
    except Exception as exc:
        log.warning("Smyths: OS DNS failed (%s) — falling back to %s", exc, IMPERVA_DNS)
    try:
        import dns.resolver
        r = dns.resolver.Resolver(configure=False)
        r.nameservers = [IMPERVA_DNS]
        r.timeout = 5
        r.lifetime = 5
        answers = r.resolve("www.smythstoys.com", "A")
        ips = [str(a) for a in answers]
        if ips:
            log.info("Smyths: resolved %s via %s — pinning Chromium to it", ips[0], IMPERVA_DNS)
            return ips[0]
    except Exception as exc:
        log.error("Smyths: external DNS via %s failed: %s — Smyths checks will fail", IMPERVA_DNS, exc)
    return None


# ─── BROWSER FACTORY ──────────────────────────────────────────────────────────

async def make_context(browser) -> BrowserContext:
    # Patchright already does its own deep stealth; add_init_script tweaks here BREAK
    # navigation against Imperva-protected sites (causes ERR_NAME_NOT_RESOLVED). Don't add one.
    return await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport=random.choice(VIEWPORTS),
        locale="en-GB",
        timezone_id="Europe/London",
        java_script_enabled=True,
    )


def _parse_proxy_url(url: str) -> dict | None:
    """Convert PROXY_URL into the dict shape Playwright expects: {server, username?, password?}."""
    if not url:
        return None
    from urllib.parse import urlparse
    u = urlparse(url)
    if not u.hostname:
        return None
    server = f"{u.scheme or 'http'}://{u.hostname}"
    if u.port:
        server += f":{u.port}"
    cfg: dict = {"server": server}
    if u.username:
        cfg["username"] = u.username
    if u.password:
        cfg["password"] = u.password
    return cfg


async def launch_chromium(pw, host_resolver_rules: str | None = None):
    args = [
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--disable-accelerated-2d-canvas",
        "--no-first-run",
        "--no-zygote",
        "--disable-gpu",
    ]
    if host_resolver_rules:
        args.append(f"--host-resolver-rules={host_resolver_rules}")
    kwargs: dict = {"headless": True, "args": args}
    proxy = _parse_proxy_url(PROXY_URL)
    if proxy:
        kwargs["proxy"] = proxy
    return await pw.chromium.launch(**kwargs)


# ─── SMYTHS ───────────────────────────────────────────────────────────────────

_SMYTHS_FETCH_JS = """
async ({path}) => {
    try {
        const r = await fetch(path, { headers: { 'Accept': 'application/json' }, credentials: 'include' });
        if (r.status !== 200) return { __err: r.status };
        return await r.json();
    } catch (e) {
        return { __err: 'fetch:' + (e && e.message || e) };
    }
}
"""

async def _smyths_api_call(page, path: str) -> dict | None:
    """Call a Smyths internal API from inside the page. Returns parsed JSON, or None on failure."""
    try:
        result = await page.evaluate(_SMYTHS_FETCH_JS, {"path": path})
    except Exception as exc:
        log.warning("Smyths API eval failed for %s: %s", path, exc)
        return None
    if isinstance(result, dict) and "__err" in result:
        return None
    return result


async def check_smyths(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    """For each Smyths product URL, fetch online + Slough store stock via in-page API.
    Imperva serves a JS challenge interstitial on first paint — we wait ~12s for
    sensor cookies to be set, then call the internal APIs which honour those cookies.
    Inventory endpoint occasionally still 403s independently — treated as best-effort."""
    if "smyths" in DISABLED:
        return state
    if not SMYTHS_PRODUCT_URLS:
        log.info("Smyths: no products configured (SMYTHS_PRODUCT_URLS empty)")
        return state

    log.info("Checking Smyths (%d products)...", len(SMYTHS_PRODUCT_URLS))
    smyths_state: dict[str, dict] = state.setdefault("smyths", {})

    for url in SMYTHS_PRODUCT_URLS:
        pid = smyths_id_from_url(url)
        if not pid:
            log.warning("Smyths: cannot extract id from %s", url)
            continue

        page = await context.new_page()
        store_data: dict | None = None
        inv_data: dict | None = None
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=35_000)
            await page.wait_for_timeout(12_000)  # let Imperva sensor cookies settle

            store_path = (
                f"/api/uk/en-gb/store-pickup/pointOfServices?productId={pid}"
                f"&selectedStore=Northampton&latitude={STORE_LAT}&longitude={STORE_LNG}"
                f"&searchThroughGeoPointFirst=true&cartPage=false"
            )
            inv_path = f"/api/uk/en-gb/product/product-inventory?code={pid}&userId=anonymous&bundle=false"

            store_data = await _smyths_api_call(page, store_path)
            if store_data is None:
                # One retry after another sensor-cookie window
                await page.wait_for_timeout(6_000)
                store_data = await _smyths_api_call(page, store_path)

            inv_data = await _smyths_api_call(page, inv_path)
        except Exception as exc:
            log.error("Smyths %s: %s", pid, exc)
        finally:
            try:
                await page.close()
            except Exception:
                pass

        if not store_data:
            log.warning("Smyths %s: store-pickup API blocked (Imperva 403) — skipping this round", pid)
            continue

        title = smyths_state.get(pid, {}).get("title") or f"Smyths #{pid}"
        # Try to lift product title from inventory payload if we got it
        if isinstance(inv_data, dict):
            t = inv_data.get("name") or inv_data.get("title") or (inv_data.get("product") or {}).get("name")
            if t:
                title = t

        stores = store_data.get("stores", []) or []
        slough = next((s for s in stores if s.get("name", "").lower() == STORE_NAME_SMYTHS.lower()), None)
        store_status = slough.get("stockLevelStatusCode") or slough.get("stockStatusMessage") or "UNKNOWN" if slough else "NO_STORE"

        hd = (inv_data or {}).get("hdSection", {}) if isinstance(inv_data, dict) else {}
        online_status = hd.get("stockLevelStatus") or hd.get("stockLevel") or "UNKNOWN"
        expected_date = hd.get("expectedStockDate", "")

        prev = smyths_state.get(pid, {})
        prev_store = prev.get("store_status", "OUTOFSTOCK")
        prev_online = prev.get("online_status", "OUTOFSTOCK")
        prev_expected = prev.get("expected", "")

        log.info("Smyths %s [%s]: store=%s online=%s exp=%s",
                 pid, title[:40], store_status, online_status, expected_date)

        # Slough store transitions
        if store_status not in ("OUTOFSTOCK", "NO_STORE", "UNKNOWN") and prev_store in ("OUTOFSTOCK", "", "NO_STORE", "UNKNOWN"):
            await send_telegram(
                f"🚨 <b>SMYTHS SLOUGH — IN STOCK</b>\n\n"
                f"{title}\n"
                f"Store: <b>{store_status}</b>\n"
                f"<a href=\"{url}\">Buy now →</a>",
                client,
            )
        elif store_status == "OUTOFSTOCK" and prev_store not in ("OUTOFSTOCK", "", "NO_STORE", "UNKNOWN"):
            await send_telegram(f"ℹ️ Smyths Slough: <i>{title}</i> back out of stock.", client)

        # Online transitions
        online_in = isinstance(online_status, str) and online_status.upper() not in ("OUTOFSTOCK", "UNKNOWN", "")
        prev_online_in = isinstance(prev_online, str) and prev_online.upper() not in ("OUTOFSTOCK", "UNKNOWN", "")
        if online_in and not prev_online_in:
            await send_telegram(
                f"🚨 <b>SMYTHS ONLINE — IN STOCK</b>\n\n"
                f"{title}\n"
                f"Status: <b>{online_status}</b>\n"
                f"<a href=\"{url}\">Buy now →</a>",
                client,
            )
        elif not online_in and prev_online_in:
            await send_telegram(f"ℹ️ Smyths online: <i>{title}</i> back out of stock.", client)

        if expected_date and prev_expected and expected_date != prev_expected:
            await send_telegram(
                f"📅 <b>Smyths date changed</b>\n\n{title}\nWas: {prev_expected}\nNow: <b>{expected_date}</b>",
                client,
            )

        smyths_state[pid] = {
            "title": title,
            "url": url,
            "store_status": store_status,
            "online_status": online_status,
            "expected": expected_date,
        }

    state["smyths"] = smyths_state
    return state


# ─── ARGOS ────────────────────────────────────────────────────────────────────

def _argos_parse_html(html: str) -> tuple[str, dict]:
    """Extract title, price, and online availability from the Argos product page HTML.

    Stock signal: the `globallyOutOfStock` boolean embedded in the inline product-state JSON.
    `false` = sellable somewhere on Argos right now. `true` = nationally OOS.

    Why not other signals:
      - JSON-LD's `availability` field is often missing on Argos
      - `<button>Add to trolley</button>` is rendered client-side, not in static HTML —
        regex-matching the literal string `add to trolley` finds JS bundle vars, not the real button
      - `deliverable: true` is a static product attribute (sells via delivery channel),
        not a real-time stock flag
      - `/finder-api/...` (the per-postcode/per-store check) is hard-blocked by Akamai

    This signal cannot tell us about Slough store stock — that requires the blocked finder-api.
    """
    title = ""
    online = {"available": False, "price": ""}

    # Title + price from JSON-LD @graph
    for ld_match in re.finditer(
        r'<script type="application/ld\+json"[^>]*>(.+?)</script>', html, re.DOTALL
    ):
        try:
            d = json.loads(ld_match.group(1))
        except Exception:
            continue
        graph = d.get("@graph") if isinstance(d, dict) else None
        items = graph if isinstance(graph, list) else [d]
        for it in items:
            if isinstance(it, dict) and it.get("@type") == "Product":
                title = title or it.get("name", "")
                offer = it.get("offers")
                if isinstance(offer, dict):
                    price = offer.get("price")
                    if price:
                        online["price"] = f"£{price}"

    # Live stock: extract `globallyOutOfStock` from inline state JSON
    m = re.search(r'"globallyOutOfStock"\s*:\s*(true|false)', html)
    if m:
        online["available"] = (m.group(1) == "false")
    else:
        # Couldn't find the flag — be conservative, treat as unknown / OOS
        online["available"] = False

    return title, online


def _argos_extract(payload: dict | list) -> tuple[str, dict, dict]:
    """Best-effort parse of Argos finder-api response. Returns (title, online_info, store_info_for_slough).
    online_info  = {"available": bool, "price": str}
    store_info   = {"name": str, "stockLevel": str, "isInStock": bool}  (or {})"""
    title = ""
    online = {"available": False, "price": ""}
    store: dict[str, Any] = {}

    # Drill into common shapes
    data = payload
    if isinstance(payload, dict):
        if "response" in payload and isinstance(payload["response"], dict):
            data = payload["response"].get("data", payload["response"])
        elif "data" in payload:
            data = payload["data"]

    items = data if isinstance(data, list) else [data]

    for item in items:
        if not isinstance(item, dict):
            continue
        prod = item.get("product") or item.get("attributes") or item
        if isinstance(prod, dict):
            title = prod.get("name") or prod.get("title") or title
            price = prod.get("price") or prod.get("nowPrice") or ""
            if isinstance(price, dict):
                price = price.get("now") or price.get("amount") or ""
            if price:
                online["price"] = f"£{price}" if not str(price).startswith("£") else str(price)
            # Online availability flags vary
            for k in ("deliverable", "deliveryAvailable", "isAvailable", "isInStock"):
                if prod.get(k) is True:
                    online["available"] = True
                    break
            online_status = prod.get("onlineStockStatus") or prod.get("availability")
            if isinstance(online_status, str) and online_status.upper() in ("INSTOCK", "IN_STOCK", "AVAILABLE"):
                online["available"] = True

        # Stores list — varied keys
        stores = (
            item.get("stores")
            or item.get("storeStock")
            or (item.get("availability", {}) or {}).get("stores")
            or []
        )
        if isinstance(stores, list):
            for s in stores:
                if not isinstance(s, dict):
                    continue
                name = (s.get("name") or s.get("storeName") or "").strip()
                if STORE_NAME_ARGOS.lower() in name.lower():
                    stk = s.get("stock") or s
                    store = {
                        "name": name,
                        "stockLevel": stk.get("stockLevel") or stk.get("level") or "",
                        "isInStock": bool(stk.get("isInStock") or stk.get("inStock") or
                                          (stk.get("stockLevel", "").upper() in ("GREEN", "AMBER"))),
                    }
                    break
        if title or store:
            break

    return title, online, store


async def check_argos(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    """Argos: plain httpx GET of the product page (NOT a browser).
    Akamai blocks Chromium fingerprints + the finder-api endpoint specifically, but
    serves the product page HTML happily to a regular HTTP client through a UK residential proxy.
    Per-store stock isn't accessible — we monitor online stock only."""
    if "argos" in DISABLED:
        return state
    if not ARGOS_PRODUCT_URLS:
        log.info("Argos: no products configured (ARGOS_PRODUCT_URLS empty)")
        return state

    log.info("Checking Argos (%d products)...", len(ARGOS_PRODUCT_URLS))
    argos_state: dict[str, dict] = state.setdefault("argos", {})

    proxy_kwargs = {"proxy": PROXY_URL} if PROXY_URL else {}
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    async with httpx.AsyncClient(timeout=25, follow_redirects=True, headers=headers, **proxy_kwargs) as session:
        for url in ARGOS_PRODUCT_URLS:
            pid = argos_id_from_url(url)
            if not pid:
                log.warning("Argos: cannot extract id from %s", url)
                continue

            try:
                resp = await session.get(url)
            except Exception as exc:
                log.error("Argos %s request failed: %s", pid, exc)
                continue

            if resp.status_code != 200:
                log.warning("Argos %s: HTTP %s — proxy IP may be Akamai-blocked, will retry next round", pid, resp.status_code)
                continue

            title, online = _argos_parse_html(resp.text)
            title = title or argos_state.get(pid, {}).get("title", f"Argos #{pid}")
            store: dict[str, Any] = {}  # per-store stock not available without browser; skip

        prev = argos_state.get(pid, {})
        prev_online = bool(prev.get("online_available"))
        prev_store_in = bool(prev.get("store_in_stock"))

        log.info("Argos %s [%s]: online=%s store=%s",
                 pid, title[:40], online["available"],
                 f"{store.get('name', '-')}/{store.get('stockLevel', '-')}")

        if online["available"] and not prev_online:
            price = f" — {online['price']}" if online.get("price") else ""
            await send_telegram(
                f"🚨 <b>ARGOS ONLINE — IN STOCK</b>\n\n{title}{price}\n<a href=\"{url}\">Buy now →</a>",
                client,
            )
        elif not online["available"] and prev_online:
            await send_telegram(f"ℹ️ Argos online: <i>{title}</i> back out of stock.", client)

        if store.get("isInStock") and not prev_store_in:
            await send_telegram(
                f"🚨 <b>ARGOS {store.get('name', 'SLOUGH').upper()} — IN STOCK</b>\n\n"
                f"{title}\nLevel: <b>{store.get('stockLevel', 'YES')}</b>\n"
                f"<a href=\"{url}\">Reserve / buy →</a>",
                client,
            )
        elif not store.get("isInStock") and prev_store_in:
            await send_telegram(f"ℹ️ Argos {store.get('name', 'Slough')}: <i>{title}</i> back out of stock.", client)

        argos_state[pid] = {
            "title": title,
            "url": url,
            "online_available": online["available"],
            "online_price": online.get("price", ""),
            "store_in_stock": bool(store.get("isInStock")),
            "store_level": store.get("stockLevel", ""),
            "store_name": store.get("name", ""),
        }

    state["argos"] = argos_state
    return state


# ─── JOHN LEWIS ───────────────────────────────────────────────────────────────

def _fmt_jl(prod: dict, icon: str = "") -> str:
    icon = icon or ("✅" if prod.get("available") else "❌")
    price = prod.get("price", "")
    price_str = f" — {price}" if price and price != "N/A" else ""
    title = prod.get("title", "Unknown")
    url = prod.get("url", "")
    return f"  {icon} <a href=\"{url}\">{title}</a>{price_str}"


async def check_john_lewis(state: dict, client: httpx.AsyncClient) -> dict:
    """John Lewis: scrape the Pokemon TCG search page via httpx + proxy.
    The whole search-result HTML is server-rendered with `<article data-product-id>` cards
    containing title, price, and href. Stock signal: presence of 'out of stock' / 'unavailable'
    text within the card markup."""
    if "john_lewis" in DISABLED:
        return state

    log.info("Checking John Lewis...")
    proxy_kwargs = {"proxy": PROXY_URL} if PROXY_URL else {}
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    try:
        async with httpx.AsyncClient(timeout=25, follow_redirects=True, headers=headers, **proxy_kwargs) as session:
            resp = await session.get(JOHN_LEWIS_URL)
        if resp.status_code != 200:
            log.warning("John Lewis: HTTP %s — proxy IP may be blocked, will retry next round", resp.status_code)
            return state
    except Exception as exc:
        log.error("John Lewis request failed: %s", exc)
        return state

    soup = BeautifulSoup(resp.text, "html.parser")
    current: dict[str, dict] = {}

    for card in soup.select("article[data-product-id]"):
        pid = card.get("data-product-id") or ""
        # Title: first non-empty text from any title-like element
        title = ""
        for sel in ('[data-testid="product-title"]', '[class*="Title"]', "h2", "h3"):
            for el in card.select(sel):
                t = el.get_text(strip=True)
                if t and len(t) > 2:
                    title = t
                    break
            if title:
                break
        if not title or len(title) < 3:
            continue

        link_el = card.select_one('a[href*="/p"]')
        href = link_el.get("href", "") if link_el else ""
        url = href if href.startswith("http") else f"https://www.johnlewis.com{href}"

        card_text = card.get_text(separator=" ", strip=True)
        price_match = re.search(r"£[\d,]+(?:\.\d{2})?", card_text)
        price = price_match.group(0) if price_match else "N/A"

        lower = card_text.lower()
        available = not (
            "out of stock" in lower
            or "sold out" in lower
            or "unavailable" in lower
            or "temporarily unavailable" in lower
        )

        key = product_key(title)
        current[key] = {"title": title, "url": url, "price": price, "available": available, "product_id": pid}

    if not current:
        log.warning("John Lewis: no products found — selectors may need updating")
        return state

    log.info("John Lewis: %d products parsed", len(current))

    prev = state.get("john_lewis", {})
    first_run = len(prev) == 0

    if first_run:
        in_stock = [v for v in current.values() if v["available"]]
        out_stock = [v for v in current.values() if not v["available"]]
        lines = [f"<b>🛒 JOHN LEWIS — Monitoring Started</b>",
                 f"<i>{len(current)} Pokemon TCG products tracked</i>"]
        if in_stock:
            lines.append("\n✅ <b>In Stock:</b>")
            for p in in_stock[:25]:
                lines.append(_fmt_jl(p))
            if len(in_stock) > 25:
                lines.append(f"  …and {len(in_stock) - 25} more in-stock")
        if out_stock:
            lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)}")
        await send_telegram("\n".join(lines), client)
    else:
        new_p, restocked, went_oos = [], [], []
        for pid, prod in current.items():
            if pid not in prev:
                new_p.append(prod)
            elif prod["available"] != prev[pid].get("available"):
                (restocked if prod["available"] else went_oos).append(prod)

        if new_p:
            lines = [f"<b>🆕 JOHN LEWIS — {len(new_p)} New Product(s)</b>"]
            for p in new_p:
                lines.append(_fmt_jl(p))
            await send_telegram("\n".join(lines), client)
        if restocked:
            lines = ["<b>🟢 JOHN LEWIS — Back In Stock</b>"]
            for p in restocked:
                lines.append(_fmt_jl(p, "✅"))
            await send_telegram("\n".join(lines), client)
        if went_oos:
            lines = ["<b>🔴 JOHN LEWIS — Out of Stock</b>"]
            for p in went_oos:
                lines.append(_fmt_jl(p, "❌"))
            await send_telegram("\n".join(lines), client)
        if not (new_p or restocked or went_oos):
            log.info("John Lewis: no changes")

    state["john_lewis"] = current
    return state


# ─── VERY ─────────────────────────────────────────────────────────────────────

def _fmt_very(prod: dict, icon: str = "") -> str:
    icon = icon or ("✅" if prod.get("available") else "❌")
    price = prod.get("price", "")
    price_str = f" — {price}" if price and price != "N/A" else ""
    title = prod.get("title", "Unknown")
    url = prod.get("url", "")
    return f"  {icon} <a href=\"{url}\">{title}</a>{price_str}"


async def check_very(state: dict, client: httpx.AsyncClient) -> dict:
    """Very: scrape the Pokemon TCG search page via httpx + proxy.
    Product cards expose all metadata as data-cnstrc-* attributes (Constructor.io tagging),
    which makes parsing trivial. Listings only show buyable items, so presence == in stock."""
    if "very" in DISABLED:
        return state

    log.info("Checking Very...")
    proxy_kwargs = {"proxy": PROXY_URL} if PROXY_URL else {}
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    try:
        async with httpx.AsyncClient(timeout=25, follow_redirects=True, headers=headers, **proxy_kwargs) as session:
            resp = await session.get(VERY_URL)
        if resp.status_code != 200:
            log.warning("Very: HTTP %s — proxy IP may be blocked, will retry next round", resp.status_code)
            return state
    except Exception as exc:
        log.error("Very request failed: %s", exc)
        return state

    soup = BeautifulSoup(resp.text, "html.parser")
    current: dict[str, dict] = {}

    for card in soup.select('[data-testid="gallery-product-card"]'):
        pid = card.get("data-cnstrc-item-id") or card.get("data-tagg-id") or ""
        title = card.get("data-cnstrc-item-name") or ""
        if not title or len(title) < 3:
            continue
        price_raw = card.get("data-cnstrc-item-price") or ""
        price = f"£{price_raw}" if price_raw else "N/A"

        link_el = card.select_one("a[href]")
        href = link_el.get("href", "") if link_el else ""
        url = href if href.startswith("http") else f"https://www.very.co.uk{href}"

        # Listings only include buyable products on Very — assume in stock if present
        available = True
        # Belt-and-braces: any explicit OOS marker text inside the card overrides
        card_text = card.get_text(separator=" ", strip=True).lower()
        if "out of stock" in card_text or "sold out" in card_text or "unavailable" in card_text:
            available = False

        key = product_key(title)
        current[key] = {"title": title, "url": url, "price": price, "available": available, "product_id": pid}

    if not current:
        log.warning("Very: no products found — selectors may need updating")
        return state

    log.info("Very: %d products parsed", len(current))

    prev = state.get("very", {})
    first_run = len(prev) == 0

    if first_run:
        in_stock = [v for v in current.values() if v["available"]]
        out_stock = [v for v in current.values() if not v["available"]]
        lines = [f"<b>🟣 VERY — Monitoring Started</b>",
                 f"<i>{len(current)} Pokemon TCG products tracked</i>"]
        if in_stock:
            lines.append("\n✅ <b>In Stock:</b>")
            for p in in_stock[:25]:
                lines.append(_fmt_very(p))
            if len(in_stock) > 25:
                lines.append(f"  …and {len(in_stock) - 25} more in-stock")
        if out_stock:
            lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)}")
        await send_telegram("\n".join(lines), client)
    else:
        new_p, restocked, went_oos = [], [], []
        prev_ids = {v.get("product_id") for v in prev.values()}
        cur_ids = {v.get("product_id") for v in current.values()}
        for pid, prod in current.items():
            if pid not in prev:
                # If it's a brand-new product (not in prev at all), flag as new
                new_p.append(prod)
            elif prod["available"] != prev[pid].get("available"):
                (restocked if prod["available"] else went_oos).append(prod)
        # Products that disappeared from the listing → treat as no-longer-available
        for pid in prev:
            if pid not in current:
                went_oos.append(prev[pid])

        if new_p:
            lines = [f"<b>🆕 VERY — {len(new_p)} New Product(s)</b>"]
            for p in new_p:
                lines.append(_fmt_very(p))
            await send_telegram("\n".join(lines), client)
        if restocked:
            lines = ["<b>🟢 VERY — Back In Stock</b>"]
            for p in restocked:
                lines.append(_fmt_very(p, "✅"))
            await send_telegram("\n".join(lines), client)
        if went_oos:
            lines = ["<b>🔴 VERY — No Longer Listed / Out of Stock</b>"]
            for p in went_oos:
                lines.append(_fmt_very(p, "❌"))
            await send_telegram("\n".join(lines), client)
        if not (new_p or restocked or went_oos):
            log.info("Very: no changes")

    state["very"] = current
    return state


# ─── MENKIND ──────────────────────────────────────────────────────────────────

def _fmt_menkind(prod: dict, icon: str = "") -> str:
    # Menkind is on BigCommerce, but its legacy cart.php permalinks are disabled —
    # any cart-add URL just redirects to the homepage. So we only ever link to the product page.
    icon = icon or ("✅" if prod.get("available") else "❌")
    price = prod.get("price", "")
    price_str = f" — {price}" if price and price != "N/A" else ""
    title = prod.get("title", "Unknown")
    url = prod.get("url", "")
    return f"  {icon} <a href=\"{url}\">{title}</a>{price_str}"


async def check_menkind(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    if "menkind" in DISABLED:
        return state

    log.info("Checking Menkind...")
    page = None
    try:
        await asyncio.sleep(random.uniform(1, 3))
        page = await context.new_page()
        await page.goto(MENKIND_URL, wait_until="domcontentloaded", timeout=35_000)
        try:
            await page.wait_for_selector("article.product-card", timeout=15_000)
        except Exception:
            log.warning("Menkind: timed out waiting for product cards")
        await asyncio.sleep(random.uniform(1, 2))

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        current: dict[str, dict] = {}

        for item in soup.select("article.product-card"):
            title_el = item.select_one("h1.product-card__title, .product-card__title-container")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            link_el = item.select_one("a.product-card__link, a[href*='/']")
            href = link_el.get("href", "") if link_el else ""
            url = href if href.startswith("http") else f"https://www.menkind.co.uk{href}"
            url = url.split("?")[0]

            price_el = item.select_one(".product-card__price")
            price = price_el.get_text(strip=True) if price_el else "N/A"

            oos_el = item.select_one("[class*='sold-out'], [class*='out-of-stock'], [class*='unavailable']")
            available = not oos_el

            key = product_key(title)
            current[key] = {
                "title": title,
                "url": url,
                "price": price,
                "available": available,
            }

        if not current:
            log.warning("Menkind: no products found — selectors may need updating")
            return state

        prev = state.get("menkind", {})
        first_run = len(prev) == 0

        if first_run:
            in_stock = [v for v in current.values() if v["available"]]
            out_stock = [v for v in current.values() if not v["available"]]
            lines = [f"<b>🎁 MENKIND — Monitoring Started</b>",
                     f"<i>{len(current)} Pokemon TCG-related products tracked</i>"]
            if in_stock:
                lines.append("\n✅ <b>In Stock:</b>")
                for p in in_stock[:25]:
                    lines.append(_fmt_menkind(p))
                if len(in_stock) > 25:
                    lines.append(f"  …and {len(in_stock) - 25} more in-stock")
            if out_stock:
                lines.append(f"\n❌ <b>Out of Stock:</b> {len(out_stock)}")
            await send_telegram("\n".join(lines), client)
        else:
            new_p, restocked, went_oos = [], [], []
            for pid, prod in current.items():
                if pid not in prev:
                    new_p.append(prod)
                elif prod["available"] != prev[pid].get("available"):
                    (restocked if prod["available"] else went_oos).append(prod)

            if new_p:
                lines = [f"<b>🆕 MENKIND — {len(new_p)} New Product(s)</b>"]
                for p in new_p:
                    lines.append(_fmt_menkind(p))
                await send_telegram("\n".join(lines), client)
            if restocked:
                lines = ["<b>🟢 MENKIND — Back In Stock</b>"]
                for p in restocked:
                    lines.append(_fmt_menkind(p, "✅"))
                await send_telegram("\n".join(lines), client)
            if went_oos:
                lines = ["<b>🔴 MENKIND — Out of Stock</b>"]
                for p in went_oos:
                    lines.append(_fmt_menkind(p, "❌"))
                await send_telegram("\n".join(lines), client)
            if not (new_p or restocked or went_oos):
                log.info("Menkind: no changes")

        state["menkind"] = current

    except Exception as exc:
        log.error("Menkind check failed: %s", exc)
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass

    return state


# ─── MONITOR LOOP ─────────────────────────────────────────────────────────────

BROWSER_REFRESH = 3_600  # rotate context hourly

async def monitor_loop(client: httpx.AsyncClient, browser, smyths_browser) -> None:
    state = load_state()

    # Always re-baseline Menkind on each start so user gets a fresh in-stock list
    state["menkind"] = {}

    CHECK_STATUS: dict[str, dict] = {
        "smyths":     {"label": "🧸 Smyths",     "ok": None, "time": "", "interval": INTERVAL_SMYTHS},
        "argos":      {"label": "🛍️ Argos",      "ok": None, "time": "", "interval": INTERVAL_ARGOS},
        "menkind":    {"label": "🎁 Menkind",    "ok": None, "time": "", "interval": INTERVAL_MENKIND},
        "john_lewis": {"label": "🛒 John Lewis", "ok": None, "time": "", "interval": INTERVAL_JOHN_LEWIS},
        "very":       {"label": "🟣 Very",       "ok": None, "time": "", "interval": INTERVAL_VERY},
    }
    status_msg_id: int | None = state.get("status_msg_id")

    def _fmt_status() -> str:
        lines = ["<b>📊 UK Monitor Status</b>"]
        for k, v in CHECK_STATUS.items():
            if k in DISABLED:
                continue
            icon = "✅" if v["ok"] is True else ("❌" if v["ok"] is False else "⏳")
            t = f" <i>({v['time']})</i>" if v["time"] else ""
            interval_min = max(1, v["interval"] // 60)
            lines.append(f"{icon} {v['label']} — every {interval_min} min{t}")
        return "\n".join(lines)

    async def _push_status() -> None:
        nonlocal status_msg_id
        text = _fmt_status()
        if status_msg_id:
            await edit_telegram(status_msg_id, text, client)
        else:
            status_msg_id = await send_telegram(text, client)
            state["status_msg_id"] = status_msg_id
            save_state(state)

    def _mark(site: str, ok: bool) -> None:
        if site in CHECK_STATUS:
            CHECK_STATUS[site]["ok"] = ok
            CHECK_STATUS[site]["time"] = time.strftime("%H:%M")

    FAIL_COUNTS: dict[str, int] = {}
    FAIL_ALERTED: dict[str, bool] = {}

    async def _track_failure(site: str, err: str) -> None:
        FAIL_COUNTS[site] = FAIL_COUNTS.get(site, 0) + 1
        if FAIL_COUNTS[site] >= 3 and not FAIL_ALERTED.get(site):
            await send_telegram(
                f"⚠️ <b>{site.upper()} — FAILING</b>\n\n{FAIL_COUNTS[site]} consecutive failures.\n<code>{err[:200]}</code>",
                client,
            )
            FAIL_ALERTED[site] = True

    def _track_success(site: str) -> None:
        FAIL_COUNTS[site] = 0
        FAIL_ALERTED[site] = False

    last = {"smyths": 0.0, "argos": 0.0, "menkind": 0.0, "john_lewis": 0.0, "very": 0.0, "rotate": 0.0}

    context = await make_context(browser)
    smyths_context = await make_context(smyths_browser) if smyths_browser else context

    await _push_status()

    try:
        while True:
            now = time.monotonic()
            HEARTBEAT["last"] = now

            if now - last["rotate"] >= BROWSER_REFRESH:
                for c in (context, smyths_context if smyths_browser else None):
                    if c is None:
                        continue
                    try:
                        await c.close()
                    except Exception:
                        pass
                context = await make_context(browser)
                smyths_context = await make_context(smyths_browser) if smyths_browser else context
                last["rotate"] = now
                log.info("Browser contexts rotated")

            # ── Smyths ────────────────────────────────────────────────
            if "smyths" not in DISABLED and SMYTHS_PRODUCT_URLS and now - last["smyths"] >= INTERVAL_SMYTHS:
                try:
                    state = await check_smyths(state, client, smyths_context)
                    _mark("smyths", True)
                    _track_success("smyths")
                except Exception as exc:
                    log.error("Smyths loop error: %s", exc)
                    _mark("smyths", False)
                    await _track_failure("smyths", str(exc))
                    # If browser context died, recreate for next round
                    if "Connection closed" in str(exc) or "Target page" in str(exc):
                        try:
                            await smyths_context.close()
                        except Exception:
                            pass
                        smyths_context = await make_context(smyths_browser) if smyths_browser else await make_context(browser)
                save_state(state)
                last["smyths"] = now
                await _push_status()
                await asyncio.sleep(random.uniform(1, 3))

            # ── Argos (in-page fetch via headless Chrome) ─────────────
            if "argos" not in DISABLED and ARGOS_PRODUCT_URLS and now - last["argos"] >= INTERVAL_ARGOS:
                try:
                    state = await check_argos(state, client, context)
                    _mark("argos", True)
                    _track_success("argos")
                except Exception as exc:
                    log.error("Argos loop error: %s", exc)
                    _mark("argos", False)
                    await _track_failure("argos", str(exc))
                    if "Connection closed" in str(exc) or "Target page" in str(exc):
                        try:
                            await context.close()
                        except Exception:
                            pass
                        context = await make_context(browser)
                save_state(state)
                last["argos"] = now
                await _push_status()
                await asyncio.sleep(random.uniform(1, 3))

            # ── Very (HTTP only — no browser) ─────────────────────────
            if "very" not in DISABLED and now - last["very"] >= INTERVAL_VERY:
                try:
                    state = await check_very(state, client)
                    _mark("very", bool(state.get("very")))
                    _track_success("very")
                except Exception as exc:
                    log.error("Very loop error: %s", exc)
                    _mark("very", False)
                    await _track_failure("very", str(exc))
                save_state(state)
                last["very"] = now
                await _push_status()
                await asyncio.sleep(random.uniform(1, 3))

            # ── John Lewis (HTTP only — no browser) ───────────────────
            if "john_lewis" not in DISABLED and now - last["john_lewis"] >= INTERVAL_JOHN_LEWIS:
                try:
                    state = await check_john_lewis(state, client)
                    _mark("john_lewis", bool(state.get("john_lewis")))
                    _track_success("john_lewis")
                except Exception as exc:
                    log.error("John Lewis loop error: %s", exc)
                    _mark("john_lewis", False)
                    await _track_failure("john_lewis", str(exc))
                save_state(state)
                last["john_lewis"] = now
                await _push_status()
                await asyncio.sleep(random.uniform(1, 3))

            # ── Menkind ───────────────────────────────────────────────
            if "menkind" not in DISABLED and now - last["menkind"] >= INTERVAL_MENKIND:
                try:
                    state = await check_menkind(state, client, context)
                    _mark("menkind", bool(state.get("menkind")))
                    _track_success("menkind")
                except Exception as exc:
                    log.error("Menkind loop error: %s", exc)
                    _mark("menkind", False)
                    await _track_failure("menkind", str(exc))
                    if "Connection closed" in str(exc) or "Target page" in str(exc):
                        try:
                            await context.close()
                        except Exception:
                            pass
                        context = await make_context(browser)
                save_state(state)
                last["menkind"] = now
                await _push_status()
                await asyncio.sleep(random.uniform(1, 3))

            await asyncio.sleep(10)

    except asyncio.CancelledError:
        log.info("Monitor loop cancelled")
    finally:
        for c in (context, smyths_context if smyths_browser else None):
            if c is None:
                continue
            try:
                await c.close()
            except Exception:
                pass


# ─── TELEGRAM LISTENER ────────────────────────────────────────────────────────

AUTOSTART = os.environ.get("AUTOSTART", "true").lower() == "true"


async def telegram_listener(client: httpx.AsyncClient, browser, smyths_browser) -> None:
    monitor_task: asyncio.Task | None = None
    watchdog_task: asyncio.Task | None = None

    # Skip messages that arrived before startup
    updates = await poll_telegram(0, client)
    offset = (updates[-1]["update_id"] + 1) if updates else 0
    log.info("Telegram listener ready (skipped %d old msg(s))", len(updates))

    await send_telegram(
        "🤖 <b>UK Pokemon TCG Monitor online.</b>\n\n"
        f"Tracking: Smyths ({len(SMYTHS_PRODUCT_URLS)}) · Argos ({len(ARGOS_PRODUCT_URLS)}) · Menkind · John Lewis · Very\n"
        f"Slough postcode: <code>{STORE_POSTCODE}</code>\n\n"
        "Send <code>start</code> to begin, <code>stop</code> to pause, <code>status</code> for state.",
        client,
    )

    async def _start() -> None:
        nonlocal monitor_task, watchdog_task
        if monitor_task and not monitor_task.done():
            await send_telegram("⚠️ Already running.", client)
            return
        HEARTBEAT["last"] = 0.0
        monitor_task = asyncio.create_task(monitor_loop(client, browser, smyths_browser))
        watchdog_task = asyncio.create_task(run_watchdog(monitor_task, client))
        await send_telegram(
            "✅ <b>Monitor started</b>\n\n"
            f"🧸 Smyths every {INTERVAL_SMYTHS // 60} min\n"
            f"🛍️ Argos every {INTERVAL_ARGOS // 60} min\n"
            f"🎁 Menkind every {INTERVAL_MENKIND // 60} min\n"
            f"🛒 John Lewis every {INTERVAL_JOHN_LEWIS // 60} min\n"
            f"🟣 Very every {INTERVAL_VERY // 60} min",
            client,
        )

    if AUTOSTART:
        log.info("AUTOSTART enabled — starting monitor immediately")
        await _start()

    while True:
        updates = await poll_telegram(offset, client)
        for upd in updates:
            offset = upd["update_id"] + 1
            msg = upd.get("message", {})
            chat_id = str(msg.get("chat", {}).get("id", ""))
            text = msg.get("text", "").strip().lower()
            if chat_id != str(TELEGRAM_CHAT_ID):
                continue
            log.info("TG cmd: %r", text)

            if text == "start":
                await _start()
            elif text == "stop":
                if not monitor_task or monitor_task.done():
                    await send_telegram("⚠️ Not running.", client)
                else:
                    monitor_task.cancel()
                    if watchdog_task and not watchdog_task.done():
                        watchdog_task.cancel()
                    await asyncio.sleep(1)
                    await send_telegram("🛑 <b>Monitor stopped.</b>", client)
            elif text == "status":
                running = monitor_task and not monitor_task.done()
                await send_telegram(
                    f"<b>Status:</b> {'🟢 Running' if running else '🔴 Stopped'}",
                    client,
                )
            else:
                await send_telegram(
                    "Commands:\n  <code>start</code>\n  <code>stop</code>\n  <code>status</code>",
                    client,
                )


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info("=" * 60)
    log.info("UK Pokemon TCG Monitor — starting")
    log.info("Smyths products: %d  Argos products: %d", len(SMYTHS_PRODUCT_URLS), len(ARGOS_PRODUCT_URLS))
    log.info("Postcode: %s   Disabled: %s", STORE_POSTCODE, sorted(DISABLED) or "none")
    log.info("Telegram: %s", "ENABLED" if TELEGRAM_ENABLED else "DISABLED (will log to stdout)")
    log.info("=" * 60)

    # When PROXY_URL is set, the proxy handles DNS — no need to pin Smyths to an IP.
    smyths_ip = (
        resolve_smyths_ip()
        if (SMYTHS_PRODUCT_URLS and "smyths" not in DISABLED and not PROXY_URL)
        else None
    )
    host_resolver_rules = f"MAP www.smythstoys.com {smyths_ip}" if smyths_ip else None
    if PROXY_URL:
        log.info("PROXY_URL set — routing all Chromium traffic through %s",
                 PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL)
    elif host_resolver_rules:
        log.info("Smyths Chromium will use host-resolver-rules: %s", host_resolver_rules)

    async with httpx.AsyncClient(timeout=35, follow_redirects=True) as client:
        async with async_playwright() as pw:
            browser = await launch_chromium(pw)
            smyths_browser = await launch_chromium(pw, host_resolver_rules) if host_resolver_rules else None
            try:
                await telegram_listener(client, browser, smyths_browser)
            except (KeyboardInterrupt, asyncio.CancelledError):
                log.info("Shutting down")
            finally:
                for b in (browser, smyths_browser):
                    if b is None:
                        continue
                    try:
                        await b.close()
                    except Exception:
                        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
