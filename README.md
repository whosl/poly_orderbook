# Poly Orderbook

Low-latency Polymarket 15-minute market collector.

## What it does

- Subscribes to the Polymarket websocket market channel
- Tracks the current ETH and BTC 15m `Up/Down` markets
- Persists raw websocket events and normalized top-of-book snapshots
- Stores history as compressed `.jsonl.gz` files
- Includes a replay tool for inspecting saved history

## Files

- `polymarket_eth_15m_ws.py` - live collector
- `polymarket_history_replay.py` - history reader / replay tool
- `pm2.polymarket-15m.config.js` - PM2 process config

## Install

```bash
python3 -m pip install -r requirements.txt
```

## Run

```bash
python3 polymarket_eth_15m_ws.py --slug eth-updown-15m-1775530800 --label eth15m
python3 polymarket_eth_15m_ws.py --slug btc-updown-15m-1775530800 --label btc15m
```

The PM2 config runs both collectors:

```bash
pm2 start pm2.polymarket-15m.config.js
```

## Replay

```bash
python3 polymarket_history_replay.py --slug eth-updown-15m-1775530800 meta
python3 polymarket_history_replay.py --slug eth-updown-15m-1775530800 start-book
python3 polymarket_history_replay.py --slug eth-updown-15m-1775530800 snapshot-at --at 2026-04-07T03:12:00Z
```

## Output

- `history/<label>/<slug>/events.jsonl.gz`
- `history/<label>/<slug>/snapshots.jsonl.gz`

## License

MIT
