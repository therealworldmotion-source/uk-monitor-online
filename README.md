# UK Pokemon TCG Retailer Monitor

24/7 stock monitor for Pokemon TCG products across **Smyths Toys**, **Argos**, and **Menkind**. Sends Telegram alerts on online stock changes and Slough store-stock changes. Runs as a Docker container on Railway with a 1 GB persistent volume.

> Sister project to the [UAE Retailer Monitor](https://github.com/therealworldmotion-source/retailer-monitor). Same architectural pattern — single-file async Python loop, patchright headless Chromium, Telegram bot, Railway deploy.

## What it tracks

| Retailer | Method | What's detected |
|---|---|---|
| **Smyths** | Headless Chromium → in-page `fetch()` of `/api/uk/en-gb/store-pickup/...` and `/product-inventory` | Slough store stock + online stock + expected restock date |
| **Argos** | Headless Chromium → `__NEXT_DATA__` extraction + `finder-api/product;partNumber=...` (when not Akamai-blocked) | Slough store stock + online stock + price |
| **Menkind** | Headless Chromium → BeautifulSoup over `article.product-card` (the existing pokemon TCG search result page) | New products, restocks, OOS transitions |

## Anti-bot context (read this before debugging)

- **Smyths** is behind **Imperva**. Direct connections fail with `ERR_NAME_NOT_RESOLVED` or 403. The bypass: pin `www.smythstoys.com` to its real Imperva edge IP via Chromium's `--host-resolver-rules` flag. The code does this automatically: OS resolver → fall back to 1.1.1.1 → pin. Without the pin, the page won't load at all.
- **Argos** is behind **Akamai**. The `finder-api` endpoint returns 403 even when called from inside a real browser session (Akamai requires telemetry headers we can't easily forge). Workaround: extract stock info from server-rendered `__NEXT_DATA__` JSON which is always present in the product page HTML. `finder-api` attempted opportunistically.
- **Both** sites' bot scoring is IP-reputation-based. The first dozen requests from a fresh IP work; after that, they may flag and block. Mitigation: 3-min poll interval (`INTERVAL_SMYTHS=180`, `INTERVAL_ARGOS=180`) keeps us under the radar.
- **If Railway's IPs get flagged**, set `PROXY_URL` to a residential proxy (Webshare ~$3-6/mo, Smartproxy $7/mo, IPRoyal ~$3-5/mo). When set, all Chromium traffic routes through the proxy and the Smyths DNS pin is auto-skipped.

## Configuration (env vars)

| Var | Required? | Default | Notes |
|---|---|---|---|
| `TELEGRAM_BOT_TOKEN` | yes | – | From @BotFather |
| `TELEGRAM_CHAT_ID` | yes | – | The user's chat ID (numeric) |
| `SMYTHS_PRODUCT_URLS` | for Smyths checks | empty | Comma-separated full product URLs (must contain `/p/<id>`) |
| `ARGOS_PRODUCT_URLS` | for Argos checks | empty | Comma-separated full product URLs (must contain `/product/<id>`) |
| `MENKIND_URL` | no | Pokemon TCG search URL | Override only if changing scope |
| `STORE_POSTCODE` | no | `SL2 1EX` | Postcode used by Argos finder-api |
| `STORE_LAT` | no | `51.510665` | Used by Smyths store-pickup API |
| `STORE_LNG` | no | `-0.59888` | Used by Smyths store-pickup API |
| `STORE_NAME_SMYTHS` | no | `slough` | Case-insensitive exact match against Smyths store name |
| `STORE_NAME_ARGOS` | no | `slough` | Substring match against Argos store name |
| `INTERVAL_SMYTHS` | no | `180` | Seconds between Smyths check rounds |
| `INTERVAL_ARGOS` | no | `180` | Seconds between Argos check rounds |
| `INTERVAL_MENKIND` | no | `180` | Seconds between Menkind checks |
| `DISABLED_RETAILERS` | no | empty | Comma-separated keys to skip: `smyths`, `argos`, `menkind` |
| `AUTOSTART` | no | `true` | Start monitoring immediately on boot (vs waiting for `start` Telegram cmd) |
| `DATA_DIR` | no (set by Dockerfile) | `/data` | Mount a persistent volume here |
| `PROXY_URL` | no | empty | Set if Railway IPs get flagged (e.g. `http://user:pass@p.webshare.io:80`) |
| `SMYTHS_FORCE_IP` | no | empty | Override the Imperva IP pin (e.g. `45.60.153.51`) |
| `IMPERVA_DNS` | no | `1.1.1.1` | Public DNS used as fallback when OS DNS fails |

## Telegram commands

Send these to the bot in your DM:
- `start` — begin monitoring
- `stop` — pause
- `status` — show current state

The bot sends a single status board message that gets edited as checks complete (no chat spam).

## Deploy / operations

### Deployment

Pushes to `main` auto-deploy via Railway.

### Local dev

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m patchright install chromium

export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
export SMYTHS_PRODUCT_URLS="https://www.smythstoys.com/uk/en-gb/.../p/260587,https://..."
export ARGOS_PRODUCT_URLS="https://www.argos.co.uk/product/8512239,..."
python monitor.py
```

### Adding a product

Edit the `SMYTHS_PRODUCT_URLS` or `ARGOS_PRODUCT_URLS` env var on Railway, save (auto-redeploy). No code change needed.

### Disabling a retailer temporarily

Set `DISABLED_RETAILERS=smyths` (or `argos`, `menkind`, comma-separated) on Railway and redeploy.

### When stock checks start failing

1. Check Railway logs for the specific retailer.
2. If Smyths: `Imperva 403 — skipping this round` repeatedly → Railway IP is flagged. Add `PROXY_URL`.
3. If Argos: `no usable data from finder-api or __NEXT_DATA__ — page may be Akamai-blocked` → same fix.
4. If Menkind: `no products found — selectors may need updating` → site HTML changed; update `check_menkind` selectors.

## Cost

- Railway Hobby: $5/mo base + ~$2-3 usage = **~$7-8/mo**.
- 1 GB persistent volume: ~$0.25/mo.
- (Optional) Residential proxy: $3-7/mo.
