# WhaleFlow Beta System

Real-time multi-exchange whale trade tracker with 8-signal reversal radar, absorption detection, and market regime analysis.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run streamlit_app.py
```

The dashboard opens at `http://localhost:8501`.

## Architecture

| File | Purpose |
|---|---|
| `collector.py` | Persistent background daemon — connects to 6 exchange WebSocket feeds (Hyperliquid, Binance, Bybit, OKX, Kraken, Coinbase), aggregates whale trades, evaluates reversal signals |
| `streamlit_app.py` | Thin Streamlit wrapper — starts the collector singleton, injects server state into the HTML dashboard |
| `index.html` | Dashboard HTML structure |
| `app.js` | Full frontend logic — WebSocket client, UI rendering, signal visualization |
| `styles.css` | Dashboard styling (dark glassmorphism theme) |

## Logs

All collector activity is logged to rotating files:

```
logs/collector.log       # Current log
logs/collector.log.1     # Previous rotation
logs/collector.log.2     # Older rotation
```

Rotates at 5MB, keeps 3 backups.

## State Snapshots

The collector saves a JSON snapshot of all accumulated data every 5 minutes:

```
runtime/state_snapshot.json
```

- **Auto-saved** every 5 minutes and on graceful shutdown
- **Auto-loaded** on startup — preserves data across restarts
- Uses atomic writes (temp file + rename) to prevent corruption

## Configuration

All tunable constants are in the `CONFIG` dict at the top of `collector.py`:

- Asset list and whale/mega thresholds
- Polling intervals (funding, signals, pressure, snapshots)
- Signal decay times (initiative: 5min, clustering: 3min)
- Signal thresholds (funding extreme, absorption, CVD divergence, etc.)
- Market regime parameters

## What "Healthy" Looks Like

1. **System Panel** (bottom of dashboard) shows:
   - All 6 exchange dots are **green** (connected)
   - Last Funding refresh < 30s ago
   - Last Trade refresh < 10s ago
   - Snapshot: **Loaded** (if not first run)

2. **Logs** show:
   - `WS HL connected`, `WS Binance connected`, etc. for all 6 exchanges
   - `Snapshot taken at HH:MM:SS` every 5 minutes
   - `Snapshot saved to runtime/state_snapshot.json`

3. **Market Regime** stabilizes after ~15 minutes of data collection

## Tests

```bash
python -m pytest tests/ -v
```

Currently covers: clustering detection, funding extreme threshold, and absorption condition logic.
