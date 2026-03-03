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

## How polling works

CSFloat has no public WebSocket or SSE stream. The bot uses two polling tiers so it reacts as fast as practically possible:

| Tier | Endpoint | Default interval | Purpose |
|------|----------|-----------------|---------|
| Notification poll | `/api/v1/me/notifications` | **15 s** | Fastest signal — typically reflects new trades within seconds |
| Heartbeat poll | `/api/v1/me` | **60 s** | Safety net — catches anything the notification tier missed |

After each processing cycle the bot waits 5 s then runs one more cycle to catch any trades that arrived during processing.

## Requirements

- Python **3.11+**
- Steam mobile authenticator (`shared_secret` + `identity_secret` — from your `.maFile`)
- CSFloat API key (Profile → Developer tab on csfloat.com)

## Installation

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

```bash
cp config.example.json config.json
# edit config.json with your credentials
```

| Key | Required | Description |
|-----|----------|-------------|
| `csfloat_api_key` | Yes | CSFloat API key |
| `steam_id64` | Yes | Your SteamID64 (17-digit number) |
| `steam_login` | Yes | Steam username |
| `steam_password` | Yes | Steam password |
| `shared_secret` | Yes | From `.maFile` — used for 2FA login |
| `identity_secret` | Yes | From `.maFile` — used to confirm trade offers |
| `steam_api_key` | No | Steam Web API key (improves offer fetching reliability) |
| `client_proxy` | No | Proxy for CSFloat HTTP: `socks5://user:pass@host:port` |
| `steam_use_proxy` | No | Apply the proxy to Steam requests too (default: `false`) |
| `notif_interval_seconds` | No | Notification poll interval (default: `15`) |
| `heartbeat_interval_seconds` | No | Heartbeat poll interval (default: `60`) |
| `post_run_delay_seconds` | No | Delay before post-cycle re-check (default: `5`) |

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
requirements.txt
config.example.json
```

## Credits

Inspired by [CSFloat-Auto-Trade-Patched](https://github.com/redlfox/CSFloat-Auto-Trade-Patched).
Uses [aiosteampy](https://github.com/somespecialone/aiosteampy) for async Steam interaction.
Steam-status notification flow reverse-engineered from the [CSFloat Chrome Extension](https://github.com/csfloat/extension).
