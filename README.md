# csfloat-auto-sale-and-trade

Async Python bot that fully automates the CS2 seller flow on [CSFloat](https://csfloat.com) — no browser extension required.

**Full lifecycle, hands-free:**

| Step | Action |
|------|--------|
| 1 | Detect new sales via fast notification polling |
| 2 | Bulk-accept trades on CSFloat |
| 3 | Send Steam trade offer to the buyer |
| 4 | Confirm via Steam Guard (mobile authenticator) |
| 5 | Notify CSFloat via steam-status API (mirrors the browser extension) |
| 6 | Keep CSFloat in sync with Steam in the background — trade history, offer states, rollbacks, cancel pings, blocked buyers (mirrors the CSFloat browser extension 5.19.2) |

## How polling works

CSFloat has no public WebSocket or SSE stream. The bot uses two polling tiers so it reacts as fast as practically possible:

| Tier | Endpoint | Default interval | Purpose |
|------|----------|-----------------|---------|
| Notification poll | `/api/v1/me/notifications` | **15 s** | Fastest signal — typically reflects new trades within seconds |
| Heartbeat poll | `/api/v1/me` | **60 s** | Safety net — catches anything the notification tier missed |

After each processing cycle the bot waits 5 s then runs one more cycle to catch any trades that arrived during processing.

Each cycle lists open sales via `/api/v1/me/trades/slim` (the lightweight listing the CSFloat extension uses), then fetches `/api/v1/trades/{id}` only for trades that still need a Steam offer. If the slim endpoint is unavailable for your API key, the bot falls back to the full `/api/v1/me/trades` listing.

## Trade-state sync (how sales get verified)

Every 3 minutes (same cadence as the CSFloat browser extension's trade-tracking
alarm) the bot reads your Steam trade history and trade offers through the
Steam Web API and reports anything CSFloat doesn't know yet:

| Report | Endpoint | When |
|--------|----------|------|
| Blocked buyers | `/api/v1/trades/steam-status/blocked-users` | A buyer/seller of a pending trade is on your Steam block list (checked every 10 min) |
| Trade history | `/api/v1/trades/steam-status/trade-history` | A Steam trade (Committed / Complete / Failed / rolled back) touches a pending CSFloat trade |
| Offer state | `/api/v1/trades/steam-status/offer` | A linked offer changed state (accepted, declined, expired, …) |
| Link offer | `/api/v1/trades/steam-status/new-offer` | An active offer gives away a pending trade's item but CSFloat hasn't linked it |
| Rollback | `/api/v1/trades/{id}/rollback` | Steam reversed the trade under trade protection |
| Cancel ping | `/api/v1/trades/{id}/cancel-ping` | The buyer asked to cancel and no live Steam offer exists |
| Status | `/api/v1/me/extension/status` | After every run: token SteamID and last Steam errors (`REPORT_EXTENSION_STATUS=false` to skip) |

Like the extension, it also cancels your offers that CSFloat has seen waiting
for mobile confirmation for over an hour.

The Nov 2026 Steam change added the `Committed` trade status (trade done, still
inside Steam's trade-protection window); it is included. The extension's
TLSNotary proofs are not reproduced — they need its WASM prover. Set
`TRADE_STATE_INTERVAL_SECONDS=0` to turn the sync off.

## Requirements

- Steam mobile authenticator (`shared_secret` + `identity_secret` — from your `.maFile`)
- CSFloat API key (Profile → Developer tab on csfloat.com)
- Either **Docker** (recommended — see below) **or** Python **3.11+**

## Quick start with Docker (recommended)

The easiest way to run the bot. Only Docker + Docker Compose are required — no Python setup.

```bash
git clone https://github.com/HouwyTwitch/csfloat-auto-sale-and-trade
cd csfloat-auto-sale-and-trade

# 1. Create your config from the template and fill in your credentials
cp .env.example .env
nano .env            # or any editor

# 2. Build and start (runs in the background, restarts automatically)
docker compose up -d --build

# 3. Watch the live logs
docker compose logs -f
```

That's it. Common commands:

```bash
docker compose logs -f          # follow live logs
docker compose ps               # show status
docker compose restart          # restart the bot
docker compose down             # stop and remove the container
docker compose up -d --build    # rebuild after pulling new code
```

The bot is configured entirely through the `.env` file — see [Configuration](#configuration)
for every available setting. Your Steam session cookies are persisted in a named
Docker volume (`csfloat-data`), so the bot avoids a full re-login (and Steam Guard
prompt) on every restart.

> **Updating:** `git pull` then `docker compose up -d --build`.
> **Resetting the saved Steam session:** `docker compose down -v` (the `-v` removes the volume).

## Manual installation (without Docker)

```bash
git clone https://github.com/HouwyTwitch/csfloat-auto-sale-and-trade
cd csfloat-auto-sale-and-trade
pip install -r requirements.txt
```

## Getting your credentials

### CSFloat API key
1. Log in to [csfloat.com](https://csfloat.com)
2. Go to **Profile → Developer**
3. Generate or copy your API key

### Steam secrets (`shared_secret` / `identity_secret`)
These come from your Steam Guard `.maFile`. If you set up Steam mobile authenticator via a tool like [SDA](https://github.com/Jessecar96/SteamDesktopAuthenticator), you already have a `.maFile` containing both values.

> **Never share these secrets** — they give full control over your Steam account's 2FA.

## Configuration

The bot can be configured two ways. **Environment variables take precedence over
`config.json`**, so you can use either — or mix them.

**Option A — environment variables / `.env` (recommended, used by Docker):**

```bash
cp .env.example .env
# edit .env with your credentials
```

**Option B — `config.json` (used by the manual / Python workflow):**

```bash
cp config.example.json config.json
# edit config.json with your credentials
```

| `config.json` key | `.env` variable | Required | Description |
|-------------------|-----------------|----------|-------------|
| `csfloat_api_key` | `CSFLOAT_API_KEY` | Yes | CSFloat API key |
| `steam_id64` | `STEAM_ID64` | Yes | Your SteamID64 (17-digit number) |
| `steam_login` | `STEAM_LOGIN` | Yes | Steam username |
| `steam_password` | `STEAM_PASSWORD` | Yes | Steam password |
| `shared_secret` | `STEAM_SHARED_SECRET` | Yes | From `.maFile` — used for 2FA login |
| `identity_secret` | `STEAM_IDENTITY_SECRET` | Yes | From `.maFile` — used to confirm trade offers |
| `steam_api_key` | `STEAM_API_KEY` | No | Steam Web API key (improves offer fetching reliability) |
| `client_proxy` | `CLIENT_PROXY` | No | Proxy for CSFloat HTTP: `socks5://user:pass@host:port` |
| `steam_use_proxy` | `STEAM_USE_PROXY` | No | Apply the proxy to Steam requests too (default: `false`) |
| `notif_interval_seconds` | `NOTIF_INTERVAL_SECONDS` | No | Notification poll interval (default: `15`) |
| `heartbeat_interval_seconds` | `HEARTBEAT_INTERVAL_SECONDS` | No | Heartbeat poll interval (default: `60`) |
| `post_run_delay_seconds` | `POST_RUN_DELAY_SECONDS` | No | Delay before post-cycle re-check (default: `5`) |
| `trade_state_interval_seconds` | `TRADE_STATE_INTERVAL_SECONDS` | No | Steam → CSFloat trade-state sync interval (default: `180`, `0` = off) |
| `report_extension_status` | `REPORT_EXTENSION_STATUS` | No | Send the extension status ping after each sync (default: `true`) |

## Usage

```bash
# Basic — reads config.json and cookies.json from the same directory
python main.py

# Custom config path
python main.py --config /path/to/my.json

# Write logs to a rotating file (10 MB × 5 backups) in addition to stdout
python main.py --log-file bot.log

# Verbose output
python main.py --log-level DEBUG

# Trace every CSFloat HTTP request/response body (useful for debugging)
python main.py --trace-http
```

Press **Ctrl+C** to stop gracefully. Steam session cookies are saved to `cookies.json` automatically so the bot avoids a full re-login on the next run.

## Running in production (systemd)

Create `/etc/systemd/system/csfloat-bot.service`:

```ini
[Unit]
Description=CSFloat Auto-Sale Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/home/youruser/csfloat-auto-sale-and-trade
ExecStart=/usr/bin/python3 main.py --log-file /var/log/csfloat-bot/bot.log
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
sudo mkdir -p /var/log/csfloat-bot
sudo chown youruser /var/log/csfloat-bot
sudo systemctl daemon-reload
sudo systemctl enable --now csfloat-bot
sudo journalctl -fu csfloat-bot   # follow live logs
```

## Project structure

```
main.py                   — entry point: argument parsing, logging setup, wires everything together
src/
    __init__.py
    config.py             — config loading and validation
    csfloat_client.py     — CSFloat REST API client (polling, accept, steam-status notify)
    steam_trader.py       — Steam client wrapper (login, inventory, send offer, confirm)
    processor.py          — trade lifecycle orchestration + fast notification polling loop
    trade_state.py        — background Steam → CSFloat trade tracking (mirrors the browser extension)
requirements.txt
config.example.json       — JSON config template
.env.example              — environment-variable config template (used by Docker)
Dockerfile                — container image definition
docker-compose.yml        — one-command run with persistent Steam session + auto-restart
```

## Credits

Inspired by [CSFloat-Auto-Trade-Patched](https://github.com/redlfox/CSFloat-Auto-Trade-Patched).
Uses [aiosteampy](https://github.com/somespecialone/aiosteampy) for async Steam interaction.
Steam-status notification flow reverse-engineered from the [CSFloat Chrome Extension](https://github.com/csfloat/extension).
