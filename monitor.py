#!/usr/bin/env python3
"""
UK Pokemon TCG Retailer Monitor — Railway edition
─────────────────────────────────────────────────
Tracks:
  • Smyths Toys  (per-product, Slough/Staines/Uxbridge store-stock + online)
  • @PBSTUK on X (real-time tweet alerts via Twitter API v2 — restock news)

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
from patchright.async_api import BrowserContext, async_playwright

try:
    from curl_cffi.requests import AsyncSession as CurlAsyncSession
except Exception:
    CurlAsyncSession = None  # type: ignore

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

# Slough store coordinates — used by Smyths store-pickup API to surface the Slough store first.
STORE_POSTCODE = os.environ.get("STORE_POSTCODE", "SL2 1EX")
STORE_LAT = os.environ.get("STORE_LAT", "51.510665")
STORE_LNG = os.environ.get("STORE_LNG", "-0.59888")
# Stores to watch — case-insensitive substring match against the store-pickup API's
# store names (one geo-search from the Slough coords returns all of these; they're
# within ~10 miles). Comma-separated, e.g. "slough,staines,uxbridge".
STORE_NAMES_SMYTHS = [
    s.strip().lower()
    for s in os.environ.get("STORE_NAMES_SMYTHS", "slough,staines,uxbridge").split(",")
    if s.strip()
]

# Check intervals (seconds).
INTERVAL_SMYTHS = int(os.environ.get("INTERVAL_SMYTHS", "30"))

# X / Twitter monitoring — @PBSTUK (Pokemon UK PBST restock alerts)
PBSTUK_HANDLE = os.environ.get("PBSTUK_HANDLE", "PBSTUK")
TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN", "").strip()
INTERVAL_PBSTUK = int(os.environ.get("INTERVAL_PBSTUK", "30"))

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
    return {"smyths": {}}


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

def smyths_id_from_url(url: str) -> str | None:
    m = re.search(r"/p/(\d+)", url)
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
    ctx = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport=random.choice(VIEWPORTS),
        locale="en-GB",
        timezone_id="Europe/London",
        java_script_enabled=True,
    )
    # Bandwidth: every byte on this context flows through the metered Webshare proxy.
    # Block all non-essential resource types — we only need HTML + JS (for Imperva
    # sensor cookies + the in-page API fetches). Saves ~80% of bandwidth per page load.
    async def _block_heavy(route):
        rt = route.request.resource_type
        if rt in ("image", "media", "font", "stylesheet"):
            await route.abort()
        else:
            await route.continue_()
    await ctx.route("**/*", _block_heavy)
    return ctx


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
# The store-pickup / inventory APIs are Imperva-challenged: a plain fetch() gets the
# challenge HTML back as inert text and 403s forever. Navigating the TAB to the API
# URL instead lets the challenge script execute, earn the sensor cookie, and reload
# into the real JSON — and once the cookie exists, subsequent navs are direct JSON.
# A persistent page keeps those cookies alive across 30s cycles.

_SMYTHS_PAGE: dict[str, Any] = {"page": None}


async def _get_smyths_page(context: BrowserContext):
    page = _SMYTHS_PAGE.get("page")
    if page is not None:
        try:
            if not page.is_closed():
                return page
        except Exception:
            pass
    page = await context.new_page()
    # Warm-up: load one real product page so the Imperva sensor runs in a full HTML
    # context and earns the domain-wide cookie (product pages load fine from Railway;
    # it's only the API endpoints that get challenged cold). Light interaction helps
    # the sensor score us as human.
    try:
        await page.goto(SMYTHS_PRODUCT_URLS[0], wait_until="domcontentloaded", timeout=35_000)
        await page.mouse.move(640, 300)
        await page.mouse.wheel(0, 600)
        await page.wait_for_timeout(15_000)
        await page.mouse.wheel(0, -200)
        cookies = await context.cookies("https://www.smythstoys.com")
        names = sorted(c["name"] for c in cookies)
        log.info("Smyths warm-up done — %d cookies: %s", len(names), ",".join(names)[:200])
    except Exception as exc:
        log.warning("Smyths warm-up failed: %s", str(exc)[:120])
    _SMYTHS_PAGE["page"] = page
    return page


async def _smyths_nav_json(page, url: str, challenge_wait: int = 20) -> dict | None:
    """Navigate to an API URL and parse the JSON body, riding out an Imperva challenge.
    Tries twice: challenge pages usually auto-reload into the JSON; if this one doesn't,
    the cookie it earned makes the second goto return JSON directly."""
    for attempt in (1, 2):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except Exception as exc:
            log.warning("Smyths nav failed: %s", str(exc)[:120])
            return None
        for _ in range(challenge_wait if attempt == 1 else 8):
            try:
                body = (await page.evaluate("() => document.body ? document.body.innerText : ''") or "").strip()
            except Exception:
                body = ""  # mid-reload — try again next second
            if body.startswith("{") or body.startswith("["):
                try:
                    return json.loads(body)
                except Exception:
                    pass
            await page.wait_for_timeout(1_000)
    # Both attempts exhausted — log what Imperva actually served so we can diagnose.
    try:
        html = (await page.content())[:160].replace("\n", " ")
    except Exception:
        html = "<unreadable>"
    log.warning("Smyths challenge body: %s", html)
    return None


async def check_smyths(state: dict, client: httpx.AsyncClient, context: BrowserContext) -> dict:
    """Fetch store stock (Slough/Staines/Uxbridge) + online stock for each configured Smyths product.

    Bandwidth-optimised for a 30s cadence: a persistent page navigates straight to the
    JSON APIs (~2 KB each) — no product-page loads at all. The first cycle pays the
    Imperva challenge once; the sensor cookie then lives in the page's context."""
    if "smyths" in DISABLED:
        return state
    if not SMYTHS_PRODUCT_URLS:
        log.info("Smyths: no products configured (SMYTHS_PRODUCT_URLS empty)")
        return state

    smyths_state: dict[str, dict] = state.setdefault("smyths", {})
    page = await _get_smyths_page(context)
    ok_count = 0

    try:
        for url in SMYTHS_PRODUCT_URLS:
            pid = smyths_id_from_url(url)
            if not pid:
                log.warning("Smyths: cannot extract id from %s", url)
                continue

            store_url = (
                f"https://www.smythstoys.com/api/uk/en-gb/store-pickup/pointOfServices?productId={pid}"
                f"&selectedStore=Northampton&latitude={STORE_LAT}&longitude={STORE_LNG}"
                f"&searchThroughGeoPointFirst=true&cartPage=false"
            )
            inv_url = f"https://www.smythstoys.com/api/uk/en-gb/product/product-inventory?code={pid}&userId=anonymous&bundle=false"

            store_data = await _smyths_nav_json(page, store_url)
            inv_data = await _smyths_nav_json(page, inv_url, challenge_wait=8)

            if not store_data:
                log.warning("Smyths %s: store API still challenged — skipping", pid)
                continue
            ok_count += 1

            title = smyths_state.get(pid, {}).get("title") or f"Smyths #{pid}"
            if isinstance(inv_data, dict):
                t = inv_data.get("name") or inv_data.get("title") or (inv_data.get("product") or {}).get("name")
                if t:
                    title = t

            stores = store_data.get("stores", []) or []

            def _find_store(wanted: str) -> dict | None:
                return next((s for s in stores if wanted in (s.get("name", "") or "").lower()), None)

            store_statuses: dict[str, str] = {}
            for wanted in STORE_NAMES_SMYTHS:
                st = _find_store(wanted)
                store_statuses[wanted] = (
                    (st.get("stockLevelStatusCode") or st.get("stockStatusMessage") or "UNKNOWN")
                    if st else "NO_STORE"
                )

            hd = (inv_data or {}).get("hdSection", {}) if isinstance(inv_data, dict) else {}
            online_status = hd.get("stockLevelStatus") or hd.get("stockLevel") or "UNKNOWN"
            expected_date = hd.get("expectedStockDate", "")

            prev = smyths_state.get(pid, {})
            # Migrate pre-multi-store state: old schema had a single "store_status" (Slough).
            prev_statuses: dict[str, str] = prev.get("store_statuses") or (
                {"slough": prev["store_status"]} if prev.get("store_status") else {}
            )
            prev_online = prev.get("online_status", "OUTOFSTOCK")
            prev_expected = prev.get("expected", "")

            if not prev or store_statuses != prev_statuses or online_status != prev_online or expected_date != prev_expected:
                log.info("Smyths %s [%s]: stores=%s online=%s exp=%s",
                         pid, title[:40], store_statuses, online_status, expected_date)

            OOS_LIKE = ("OUTOFSTOCK", "", "NO_STORE", "UNKNOWN")
            for wanted, store_status in store_statuses.items():
                prev_store = prev_statuses.get(wanted, "OUTOFSTOCK")
                if store_status not in OOS_LIKE and prev_store in OOS_LIKE:
                    await send_telegram(
                        f"🚨 <b>SMYTHS {wanted.upper()} — IN STOCK</b>\n\n{title}\nStore: <b>{store_status}</b>\n<a href=\"{url}\">Buy now →</a>",
                        client,
                    )
                elif store_status == "OUTOFSTOCK" and prev_store not in OOS_LIKE:
                    await send_telegram(f"ℹ️ Smyths {wanted.title()}: <i>{title}</i> back out of stock.", client)

            online_in = isinstance(online_status, str) and online_status.upper() not in ("OUTOFSTOCK", "UNKNOWN", "")
            prev_online_in = isinstance(prev_online, str) and prev_online.upper() not in ("OUTOFSTOCK", "UNKNOWN", "")
            if online_in and not prev_online_in:
                await send_telegram(
                    f"🚨 <b>SMYTHS ONLINE — IN STOCK</b>\n\n{title}\nStatus: <b>{online_status}</b>\n<a href=\"{url}\">Buy now →</a>",
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
                "store_statuses": store_statuses,
                "online_status": online_status,
                "expected": expected_date,
            }
    except Exception:
        # A dead page would poison every future cycle — drop it so the next cycle
        # opens a fresh one (and re-pays the Imperva challenge once).
        try:
            await page.close()
        except Exception:
            pass
        _SMYTHS_PAGE["page"] = None
        raise

    log.info("Smyths cycle: %d/%d products OK", ok_count, len(SMYTHS_PRODUCT_URLS))
    state["smyths"] = smyths_state
    return state


# ─── @PBSTUK X/TWITTER MONITOR ────────────────────────────────────────────────
# Polls Twitter API v2 every INTERVAL_PBSTUK seconds. New tweets get fanned to
# Telegram. Requires TWITTER_BEARER_TOKEN env var (free dev account, no proxy).


async def _pbstuk_fetch(client: httpx.AsyncClient, since_id: str | None = None) -> list[dict] | None:
    """Fetch @PBSTUK tweets via Twitter API v2.
    Pass since_id to only fetch tweets newer than that ID (zero usage when nothing new).
    Returns list of {id, link, pubDate, text} newest-first, [] if none new, None on failure."""
    if not TWITTER_BEARER_TOKEN:
        log.warning("PBSTUK: TWITTER_BEARER_TOKEN not set — cannot fetch tweets")
        return None
    params: dict = {
        "query": f"from:{PBSTUK_HANDLE} -is:retweet -is:reply",
        "tweet.fields": "created_at,text",
        "max_results": "10",
    }
    if since_id:
        params["since_id"] = since_id
    try:
        r = await client.get(
            "https://api.twitter.com/2/tweets/search/recent",
            params=params,
            headers={"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"},
            timeout=15,
        )
    except Exception as exc:
        log.warning("PBSTUK: Twitter API error: %s", exc)
        return None

    if r.status_code != 200:
        log.warning("PBSTUK: Twitter API returned %s: %s", r.status_code, r.text[:200])
        return None

    tweets = r.json().get("data") or []
    return [
        {
            "id": t["id"],
            "link": f"https://x.com/{PBSTUK_HANDLE}/status/{t['id']}",
            "pubDate": t.get("created_at", ""),
            "text": t.get("text", ""),
        }
        for t in tweets
    ]


async def check_pbstuk(state: dict, client: httpx.AsyncClient) -> dict:
    """Fan new @PBSTUK tweets to Telegram. Uses since_id so the API only returns
    genuinely new tweets — zero usage when @PBSTUK hasn't posted."""
    if "pbstuk" in DISABLED:
        return state

    newest_id: str | None = state.get("pbstuk_newest_id")

    if not newest_id:
        # First ever run — baseline silently so we don't spam old tweets.
        items = await _pbstuk_fetch(client)  # no since_id: get recent tweets
        if items is None:
            return state
        if items:
            newest_id = items[0]["id"]  # highest ID = most recent
            state["pbstuk_newest_id"] = newest_id
            latest = items[0]
            await send_telegram(
                f"🐦 <b>@{PBSTUK_HANDLE} watcher armed</b>\n\n"
                f"Baselined {len(items)} recent tweets. Will alert on new ones only.\n\n"
                f"Latest: <i>{latest['text'][:300]}</i>\n<a href=\"{latest['link']}\">view</a>",
                client,
            )
        return state

    # Normal run — only fetch tweets newer than the last one we saw.
    items = await _pbstuk_fetch(client, since_id=newest_id)
    if items is None:
        return state  # API error — try again next cycle
    if not items:
        log.info("PBSTUK: no new tweets")
        return state

    # Alert oldest-first so the chat reads in order.
    for it in reversed(items):
        await send_telegram(
            f"🐦 <b>@{PBSTUK_HANDLE}</b> · <i>{it['pubDate']}</i>\n\n"
            f"{it['text'][:1500]}\n\n<a href=\"{it['link']}\">view tweet</a>",
            client,
        )

    # Store the newest ID seen so next call only gets tweets after this.
    state["pbstuk_newest_id"] = items[0]["id"]
    return state


# ─── MONITOR LOOP ─────────────────────────────────────────────────────────────

BROWSER_REFRESH = 3_600  # rotate context hourly

async def monitor_loop(client: httpx.AsyncClient, browser) -> None:
    state = load_state()

    CHECK_STATUS: dict[str, dict] = {
        "smyths":  {"label": "🧸 Smyths",       "ok": None, "time": "", "interval": INTERVAL_SMYTHS},
        "pbstuk":  {"label": "🐦 @PBSTUK feed", "ok": None, "time": "", "interval": INTERVAL_PBSTUK},
    }
    status_msg_id: int | None = state.get("status_msg_id")

    def _fmt_interval(sec: int) -> str:
        return f"{sec}s" if sec < 60 else f"{max(1, sec // 60)} min"

    def _fmt_status() -> str:
        lines = ["<b>📊 UK Monitor Status</b>"]
        for k, v in CHECK_STATUS.items():
            if k in DISABLED:
                continue
            icon = "✅" if v["ok"] is True else ("❌" if v["ok"] is False else "⏳")
            t = f" <i>({v['time']})</i>" if v["time"] else ""
            lines.append(f"{icon} {v['label']} — every {_fmt_interval(v['interval'])}{t}")
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

    last = {"smyths": 0.0, "pbstuk": 0.0, "rotate": 0.0}

    context = await make_context(browser)

    await _push_status()

    try:
        while True:
            now = time.monotonic()
            HEARTBEAT["last"] = now

            if now - last["rotate"] >= BROWSER_REFRESH:
                try:
                    await context.close()
                except Exception:
                    pass
                context = await make_context(browser)
                # Persistent Smyths page died with the old context; next cycle recreates it.
                _SMYTHS_PAGE["page"] = None
                last["rotate"] = now
                log.info("Browser context rotated")

            # ── Smyths (persistent page, 30s cadence) ─────────────────
            if "smyths" not in DISABLED and SMYTHS_PRODUCT_URLS and now - last["smyths"] >= INTERVAL_SMYTHS:
                try:
                    state = await check_smyths(state, client, context)
                    _mark("smyths", True)
                    _track_success("smyths")
                except Exception as exc:
                    log.error("Smyths loop error: %s", exc)
                    _mark("smyths", False)
                    await _track_failure("smyths", str(exc))
                    # If browser context died, recreate for next round
                    if "Connection closed" in str(exc) or "Target page" in str(exc):
                        try:
                            await context.close()
                        except Exception:
                            pass
                        context = await make_context(browser)
                        _SMYTHS_PAGE["page"] = None
                save_state(state)
                last["smyths"] = now
                await _push_status()

            # ── @PBSTUK Twitter watcher (HTTP, no proxy, fast cadence) ─
            if "pbstuk" not in DISABLED and now - last["pbstuk"] >= INTERVAL_PBSTUK:
                try:
                    state = await check_pbstuk(state, client)
                    _mark("pbstuk", True)
                    _track_success("pbstuk")
                except Exception as exc:
                    log.error("PBSTUK loop error: %s", exc)
                    _mark("pbstuk", False)
                    await _track_failure("pbstuk", str(exc))
                save_state(state)
                last["pbstuk"] = now

            await asyncio.sleep(5)

    except asyncio.CancelledError:
        log.info("Monitor loop cancelled")
    finally:
        try:
            await context.close()
        except Exception:
            pass


# ─── TELEGRAM LISTENER ────────────────────────────────────────────────────────

AUTOSTART = os.environ.get("AUTOSTART", "true").lower() == "true"


async def telegram_listener(client: httpx.AsyncClient, browser) -> None:
    monitor_task: asyncio.Task | None = None
    watchdog_task: asyncio.Task | None = None

    # Skip messages that arrived before startup
    updates = await poll_telegram(0, client)
    offset = (updates[-1]["update_id"] + 1) if updates else 0
    log.info("Telegram listener ready (skipped %d old msg(s))", len(updates))

    await send_telegram(
        "🤖 <b>UK Pokemon TCG Monitor online.</b>\n\n"
        f"Tracking: Smyths ({len(SMYTHS_PRODUCT_URLS)} products, Slough/Staines/Uxbridge) · @{PBSTUK_HANDLE}\n\n"
        "Send <code>start</code> to begin, <code>stop</code> to pause, <code>status</code> for state.",
        client,
    )

    async def _start() -> None:
        nonlocal monitor_task, watchdog_task
        if monitor_task and not monitor_task.done():
            await send_telegram("⚠️ Already running.", client)
            return
        HEARTBEAT["last"] = 0.0
        monitor_task = asyncio.create_task(monitor_loop(client, browser))
        watchdog_task = asyncio.create_task(run_watchdog(monitor_task, client))
        await send_telegram(
            "✅ <b>Monitor started</b>\n\n"
            f"🧸 Smyths every {INTERVAL_SMYTHS}s (Slough · Staines · Uxbridge + online)\n"
            f"🐦 @{PBSTUK_HANDLE} every {INTERVAL_PBSTUK}s",
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
    log.info("Smyths products: %d every %ds  PBSTUK: every %ds", len(SMYTHS_PRODUCT_URLS), INTERVAL_SMYTHS, INTERVAL_PBSTUK)
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
            browser = await launch_chromium(pw, host_resolver_rules)
            try:
                await telegram_listener(client, browser)
            except (KeyboardInterrupt, asyncio.CancelledError):
                log.info("Shutting down")
            finally:
                try:
                    await browser.close()
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
