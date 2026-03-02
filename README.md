# csfloat-auto-sale-and-trade

Async Python bot that fully automates the CS2 seller flow on [CSFloat](https://csfloat.com):

1. **Detects** new sales via fast notification polling (no waiting 5+ minutes)
2. **Accepts** trades on CSFloat (bulk API call)
3. **Sends** a Steam trade offer to the buyer
4. **Confirms** the offer via Steam Guard (mobile authenticator)
5. CSFloat verifies the completed trade automatically

## How it works — fast polling strategy

CSFloat does not expose a public WebSocket / SSE stream, so the bot uses a
two-tier approach to react as quickly as possible:

| Tier | Endpoint | Interval | Purpose |
|------|----------|----------|---------|
| **Notification poll** | `/api/v1/me/notifications` | **15 s** | Fastest signal of a new trade |
| **Heartbeat poll** | `/api/v1/me` | **60 s** | Safety net — catches anything missed |

As soon as either tier fires, a full processing cycle runs immediately.
After each cycle the bot waits 5 s then runs one more cycle to catch
any trades that arrived during processing.

## Requirements

- Python 3.11+
- A Steam account with mobile authenticator (`shared_secret` + `identity_secret` from your `.maFile`)
- A CSFloat API key (Profile → Developer tab on csfloat.com)

## Installation

```bash
git clone https://github.com/HouwyTwitch/csfloat-auto-sale-and-trade
cd csfloat-auto-sale-and-trade
pip install -r requirements.txt
```

> **Note:** `steampy` is installed directly from [crab713/steampy](https://github.com/crab713/steampy) via pip's git support. Make sure `git` is available in your PATH.

## Configuration

```bash
cp config.example.json config.json
# edit config.json with your credentials
```

| Key | Required | Description |
|-----|----------|-------------|
| `csfloat_api_key` | Yes | CSFloat API key |
| `steam_id64` | Yes | Your SteamID64 |
| `steam_login` | Yes | Steam username |
| `steam_password` | Yes | Steam password |
| `shared_secret` | Yes | From `.maFile` — used for 2FA login |
| `identity_secret` | Yes | From `.maFile` — used to confirm trades |
| `steam_api_key` | No | Steam Web API key |
| `client_proxy` | No | Proxy URL e.g. `socks5://user:pass@host:port` |
| `steam_use_proxy` | No | Apply proxy to Steam requests too (default: false) |
| `notif_interval_seconds` | No | Notification poll interval (default: 15) |
| `heartbeat_interval_seconds` | No | Heartbeat poll interval (default: 60) |
| `post_run_delay_seconds` | No | Delay before post-cycle re-check (default: 5) |

## Usage

```bash
python main.py                          # uses config.json + cookies.json
python main.py --config my.json         # custom config path
python main.py --log-level DEBUG        # verbose output
```

Press **Ctrl+C** to stop gracefully. Steam session cookies are saved to
`cookies.json` automatically to avoid re-login on next run.

## Project structure

```
main.py            — entry point, argument parsing, wires everything together
config.py          — config loading + validation
csfloat_client.py  — CSFloat REST API wrapper (polling, accept, etc.)
steam_trader.py    — steampy wrapper (login, send offer, confirm)
processor.py       — trade lifecycle orchestration + fast polling loop
requirements.txt
config.example.json
```

## Credits

Inspired by [CSFloat-Auto-Trade-Patched](https://github.com/redlfox/CSFloat-Auto-Trade-Patched).
Uses [crab713/steampy](https://github.com/crab713/steampy) for Steam interaction.
