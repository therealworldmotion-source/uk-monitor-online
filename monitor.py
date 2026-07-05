#!/usr/bin/env python3
"""
X/Twitter → Telegram bot — Railway edition
───────────────────────────────────────────
Watches one or more X accounts (Twitter API v2, TWITTER_HANDLES env) and pushes
every new original tweet (no replies, no retweets) to Telegram within ~30s,
labelled by account. All handles share ONE combined query. No browser, no proxy.

Designed for a single Railway container. State persists to DATA_DIR
(mount a small volume at /data). All config via env vars — see README.md.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path

import httpx

# ─── ENV CONFIG ───────────────────────────────────────────────────────────────

DATA_DIR = Path(os.environ.get("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# X / Twitter monitoring — comma-separated handles (Pokemon UK restock accounts).
# All handles are checked in ONE combined query, so adding more doesn't cost extra API calls.
TWITTER_HANDLES = [
    h.strip().lstrip("@")
    for h in os.environ.get("TWITTER_HANDLES", "PBSTUK,dropalertsuk").split(",")
    if h.strip()
]
TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN", "").strip()
# INTERVAL_PBSTUK kept for backward-compat with the existing Railway env var.
INTERVAL_TWITTER = int(os.environ.get("INTERVAL_TWITTER", os.environ.get("INTERVAL_PBSTUK", "30")))

AUTOSTART = os.environ.get("AUTOSTART", "true").lower() == "true"

MAX_TG_LENGTH = 4000

# Mutable heartbeat — monitor loop writes, watchdog reads
HEARTBEAT: dict[str, float] = {"last": 0.0}

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ─── STATE ────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            log.warning("State file corrupt, starting fresh")
    return {}


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
                    "🚨 <b>Bot not responding</b>\n\nNo heartbeat for 10+ min.\nSend <code>stop</code> then <code>start</code>.",
                    client,
                )
                alerted = True
        else:
            if alerted:
                await send_telegram("✅ <b>Bot recovered.</b>", client)
            alerted = False
    if not monitor_task.cancelled():
        try:
            exc = monitor_task.exception()
            if exc:
                await send_telegram(
                    f"🚨 <b>Bot crashed</b>\n\n<code>{type(exc).__name__}: {exc}</code>\n\nSend <code>start</code> to restart.",
                    client,
                )
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass


# ─── X/TWITTER MONITOR ────────────────────────────────────────────────────────
# Polls Twitter API v2 every INTERVAL_TWITTER seconds across ALL configured handles
# in one combined query. New tweets get fanned to Telegram, each labelled with its
# account. Requires TWITTER_BEARER_TOKEN (no proxy, no browser).

async def _twitter_fetch(client: httpx.AsyncClient, since_id: str | None = None) -> list[dict] | None:
    """Fetch new tweets from all TWITTER_HANDLES via one combined API v2 query.
    Pass since_id to only fetch tweets newer than that ID (zero cost when nothing new).
    Returns list of {id, handle, link, pubDate, text} newest-first, [] if none, None on failure."""
    if not TWITTER_BEARER_TOKEN:
        log.warning("Twitter: TWITTER_BEARER_TOKEN not set — cannot fetch tweets")
        return None
    if not TWITTER_HANDLES:
        return []
    from_clause = " OR ".join(f"from:{h}" for h in TWITTER_HANDLES)
    params: dict = {
        "query": f"({from_clause}) -is:retweet -is:reply",
        "tweet.fields": "created_at,author_id",
        "expansions": "author_id",
        "user.fields": "username",
        "max_results": "20",
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
        log.warning("Twitter: API error: %s", exc)
        return None

    if r.status_code != 200:
        log.warning("Twitter: API returned %s: %s", r.status_code, r.text[:200])
        return None

    data = r.json()
    users = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}
    tweets = data.get("data") or []
    out = []
    for t in tweets:
        handle = users.get(t.get("author_id"), TWITTER_HANDLES[0])
        out.append({
            "id": t["id"],
            "handle": handle,
            "link": f"https://x.com/{handle}/status/{t['id']}",
            "pubDate": t.get("created_at", ""),
            "text": t.get("text", ""),
        })
    return out


async def check_twitter(state: dict, client: httpx.AsyncClient) -> dict:
    """Fan new tweets from all watched accounts to Telegram. Uses since_id so the API
    only returns genuinely new tweets — zero usage when nobody has posted."""
    newest_id: str | None = state.get("twitter_newest_id")

    if not newest_id:
        # First run of the multi-account watcher — baseline silently (no spam of old tweets).
        items = await _twitter_fetch(client)  # no since_id: recent tweets
        if items is None:
            return state
        if items:
            state["twitter_newest_id"] = items[0]["id"]  # highest ID = most recent
        handles = ", ".join(f"@{h}" for h in TWITTER_HANDLES)
        await send_telegram(
            f"🐦 <b>Watcher armed</b>\n\nWatching: {handles}\n"
            f"Baselined {len(items)} recent tweets — will alert on new ones only.",
            client,
        )
        return state

    # Normal run — only fetch tweets newer than the last one we saw.
    items = await _twitter_fetch(client, since_id=newest_id)
    if items is None:
        return state  # API error — try again next cycle
    if not items:
        log.info("Twitter: no new tweets")
        return state

    # Alert oldest-first so the chat reads in order.
    for it in reversed(items):
        await send_telegram(
            f"🐦 <b>@{it['handle']}</b> · <i>{it['pubDate']}</i>\n\n"
            f"{it['text'][:1500]}\n\n<a href=\"{it['link']}\">view tweet</a>",
            client,
        )
    log.info("Twitter: delivered %d new tweet(s)", len(items))

    # Store the newest ID seen so next call only gets tweets after this.
    state["twitter_newest_id"] = items[0]["id"]
    return state


# ─── MONITOR LOOP ─────────────────────────────────────────────────────────────

async def monitor_loop(client: httpx.AsyncClient) -> None:
    state = load_state()
    last_check = 0.0
    handles = ", ".join(f"@{h}" for h in TWITTER_HANDLES)

    await send_telegram(
        f"✅ <b>Monitor started</b>\n\n🐦 Watching {handles} every {INTERVAL_TWITTER}s",
        client,
    )

    try:
        while True:
            now = time.monotonic()
            HEARTBEAT["last"] = now

            if now - last_check >= INTERVAL_TWITTER:
                try:
                    state = await check_twitter(state, client)
                except Exception as exc:
                    log.error("Twitter loop error: %s", exc)
                save_state(state)
                last_check = now

            await asyncio.sleep(2)

    except asyncio.CancelledError:
        log.info("Monitor loop cancelled")


# ─── TELEGRAM LISTENER ────────────────────────────────────────────────────────

async def telegram_listener(client: httpx.AsyncClient) -> None:
    monitor_task: asyncio.Task | None = None
    watchdog_task: asyncio.Task | None = None

    # Skip messages that arrived before startup
    updates = await poll_telegram(0, client)
    offset = (updates[-1]["update_id"] + 1) if updates else 0
    log.info("Telegram listener ready (skipped %d old msg(s))", len(updates))

    handles = ", ".join(f"@{h}" for h in TWITTER_HANDLES)
    await send_telegram(
        f"🤖 <b>X → Telegram bot online.</b>\n\nWatching: {handles}\n\n"
        "Send <code>start</code> to begin, <code>stop</code> to pause, <code>status</code> for state.",
        client,
    )

    async def _start() -> None:
        nonlocal monitor_task, watchdog_task
        if monitor_task and not monitor_task.done():
            await send_telegram("⚠️ Already running.", client)
            return
        HEARTBEAT["last"] = 0.0
        monitor_task = asyncio.create_task(monitor_loop(client))
        watchdog_task = asyncio.create_task(run_watchdog(monitor_task, client))

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
                handles = ", ".join(f"@{h}" for h in TWITTER_HANDLES)
                await send_telegram(
                    f"<b>Status:</b> {'🟢 Running' if running else '🔴 Stopped'}\n"
                    f"🐦 {handles} every {INTERVAL_TWITTER}s",
                    client,
                )
            elif text == "test":
                # On-demand pipeline check — pulls the latest tweets regardless of since_id
                # so you can confirm fetch → Telegram works even when the accounts are quiet.
                items = await _twitter_fetch(client)
                if not items:
                    await send_telegram("⚠️ Test: couldn't fetch any tweets (API issue?).", client)
                else:
                    it = items[0]
                    await send_telegram(
                        f"🧪 <b>TEST — latest tweet (@{it['handle']})</b>\n"
                        f"<i>{it['pubDate']}</i>\n\n{it['text'][:1500]}\n\n"
                        f"<a href=\"{it['link']}\">view tweet</a>\n\n"
                        f"✅ Pipeline working — real new tweets arrive like this within {INTERVAL_TWITTER}s.",
                        client,
                    )
            else:
                await send_telegram(
                    "Commands:\n  <code>start</code>\n  <code>stop</code>\n  <code>status</code>\n  <code>test</code> — show latest tweet now",
                    client,
                )


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info("=" * 60)
    log.info("X → Telegram bot — starting")
    log.info("Watching: %s", ", ".join(f"@{h}" for h in TWITTER_HANDLES))
    log.info("Poll every %ds  Twitter token: %s", INTERVAL_TWITTER, "SET" if TWITTER_BEARER_TOKEN else "MISSING")
    log.info("Telegram: %s", "ENABLED" if TELEGRAM_ENABLED else "DISABLED (will log to stdout)")
    log.info("=" * 60)

    async with httpx.AsyncClient(timeout=35, follow_redirects=True) as client:
        try:
            await telegram_listener(client)
        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("Shutting down")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
