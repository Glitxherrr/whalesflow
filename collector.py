"""
WhaleFlow Persistent Data Collector v3
8-Signal Reversal Radar + Mega Whale tracking + Funding Flip
Runs as background daemon Ã¢â‚¬â€ collects continuously even when no user is viewing.
"""

import copy
import threading
import json
import time
import asyncio
import requests
import websockets
import logging
import logging.handlers
import signal as _signal
import atexit
import os
import sys
from pathlib import Path
from collections import deque
from datetime import datetime
from typing import TypedDict, List, Dict, Union, Optional, Any, Set, Deque, cast
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

# ===== TYPES =====
class Trade(TypedDict, total=False):
    coin: str
    price: float
    size: float
    side: str
    time: int
    exchange: str
    is_mega: bool
    mega_type: Optional[str]
    value: float
    cluster_count: Optional[int]

class WhaleBucket(TypedDict):
    time: int
    buy: float
    sell: float
    buy_count: int
    sell_count: int

class SignalDetail(TypedDict):
    active: bool
    side: Optional[str]
    detail: str
    time: float

class RegimeData(TypedDict):
    score: int
    label: str
    css_class: str
    last_change_time: float
    price_history: List[Dict[str, float]]
    range_score: int
    volume_score: int
    cvd_score: int
    balance_score: int

class CoinData(TypedDict):
    whale_trades: Deque[Trade]
    total_buy_vol: float
    total_sell_vol: float
    buy_count: int
    sell_count: int
    current_buy_vol: float
    current_sell_vol: float
    current_buy_count: int
    current_sell_count: int
    whale_buckets: Deque[WhaleBucket]
    mega_whales: Deque[Trade]
    abs_cum_buy: float
    abs_cum_sell: float
    abs_snapshots: Deque[Dict[str, Any]]
    abs_detected: bool
    abs_side: Optional[str]
    abs_conditions: Dict[str, bool]
    abs_conditions_met: int
    abs_metrics: Dict[str, float]
    last_trade_price: float
    funding: float
    funding_history: Deque[Dict[str, float]]
    market_history: Deque[Dict[str, float]]
    mark_px: float
    oracle_px: float
    open_interest: float
    day_volume: float
    pressure_history: Deque[Dict[str, Any]]
    last_pressure_snap: Dict[str, float]
    volume_buckets: Deque[Dict[str, float]]
    current_bucket_buy: float
    current_bucket_sell: float
    signals: Dict[str, SignalDetail]
    alert_level: int
    alert_label: str
    regime: RegimeData
    current_since: float

# ===== IN-MEMORY LOG HANDLER =====
class MemoryLogHandler(logging.Handler):
    """Stores last N log records in a deque for live UI display."""
    def __init__(self, capacity=500):
        super().__init__()
        self.buffer = deque(maxlen=capacity)
        self._broadcaster = None
    def set_broadcaster(self, fn):
        """Set callback: fn(entry_dict) â€” called on every log emit."""
        self._broadcaster = fn
    def emit(self, record):
        entry = {
            'timestamp': record.created,
            'time': self.format(record).split(' [')[0] if ' [' in self.format(record) else datetime.now().strftime('%H:%M:%S'),
            'timeShort': datetime.fromtimestamp(record.created).strftime('%I:%M:%S %p'),
            'level': record.levelname,
            'msg': record.getMessage(),
        }
        self.buffer.append(entry)
        if self._broadcaster:
            try:
                self._broadcaster(entry)
            except Exception:
                pass  # Never let broadcast errors break the logger
    def get_entries(self):
        return list(self.buffer)

# ===== PATHS =====
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / 'logs'
RUNTIME_DIR = BASE_DIR / 'runtime'

# HuggingFace Spaces mounts a persistent volume at /data that survives container
# restarts.  Use it when available so the snapshot is not lost on cold-start.
_HF_PERSISTENT = Path('/data')
SNAPSHOT_DIR = _HF_PERSISTENT if _HF_PERSISTENT.exists() else RUNTIME_DIR

# ===== LOGGING SETUP =====
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger('whaleflow')
logger.setLevel(logging.INFO)
_fh = logging.handlers.RotatingFileHandler(
    LOG_DIR / 'collector.log', maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
)
_fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(_fh)
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger.addHandler(_ch)
_mh = MemoryLogHandler(capacity=500)
_mh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(_mh)

# ===== CONFIGURATION =====
CONFIG = {
    'coins': ['BTC', 'ETH', 'SOL', 'PAXG', 'XRP'],
    'whale_thresholds': {'BTC': 50, 'ETH': 50, 'SOL': 50, 'PAXG': 50, 'XRP': 50},
    'mega_thresholds': {'BTC': 2000000, 'ETH': 1000000, 'SOL': 500000, 'PAXG': 200000, 'XRP': 300000},
    'ws_port': 7860,  # Default for Hugging Face is 7860
    'funding_poll_interval': 30,
    'signal_eval_interval': 30,
    'signal_initial_delay': 30,
    'snapshot_interval': 30,
    'snapshot_initial_delay': 15,
    'pressure_interval': 30,
    'signal_decay_initiative': 300,
    'signal_decay_clustering': 180,
    'clustering_window_ms': 60000,
    'clustering_min_trades': 5,
    'agg_window_ms': 1500,          # Aggregation time window (ms)
    'agg_min_value': 200,           # Min individual trade value ($) to buffer
    'agg_max_entries': 50000,       # Safety cap for buffer size
    'agg_flush_interval': 1.0,     # Flush frequency (seconds)
    'funding_extreme_threshold': 0.005,
    'absorption_imbalance_min': 60,
    'absorption_vol_min': 50000,
    'absorption_price_delta_max': 0.02,
    'absorption_oi_delta_min': 0.05,
    'absorption_funding_threshold': 0.000005,
    'cvd_price_threshold': 0.02,
    'cvd_ratio': 0.4,
    'oi_divergence_threshold': 0.05,
    'volume_climax_ratio': 5.0,
    'volume_climax_buy_high': 0.6,
    'volume_climax_buy_low': 0.4,
    'regime_hold_time': 900,
    'regime_thresholds': {'BTC': 0.15, 'ETH': 0.20, 'SOL': 0.35, 'XRP': 0.30, 'PAXG': 0.08},
    'snapshot_path': str(SNAPSHOT_DIR / 'state_snapshot.json'),
    'max_trade_value': 100_000_000_000_000,
    'trade_time_tolerance_ms': 300_000,
    'dedupe_cache_size': 2000,
    'funding_history_maxlen': 8640,  # 72 hours at 30s intervals
    'market_history_maxlen': 8640,   # 72 hours at 30s intervals
    'abs_snapshots_maxlen': 2880,
    'pressure_history_maxlen': 2880,
    'volume_buckets_maxlen': 2880,
}

app = FastAPI()

# Set by HyperliquidCollector.get_instance() once the singleton is fully initialised.
# _run_local_server() waits on this before calling uvicorn.run() so that the first
# WebSocket connection is never served before the collector is ready.
_server_ready = threading.Event()

_NO_CACHE = {"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}

# ── Static assets ──────────────────────────────────────────────────────────────
@app.get("/")
async def get_index():
    return FileResponse(str(BASE_DIR / "index.html"), headers=_NO_CACHE)

@app.get("/styles.css")
async def get_styles():
    return FileResponse(str(BASE_DIR / "styles.css"), media_type="text/css", headers=_NO_CACHE)

@app.get("/app.js")
async def get_app_js():
    return FileResponse(str(BASE_DIR / "app.js"), media_type="application/javascript", headers=_NO_CACHE)

# Mount static for everything else
app.mount("/static", StaticFiles(directory=str(BASE_DIR)), name="static")

# ── API routes registered at module level so they are compiled into the ASGI app
#    BEFORE uvicorn.run() is called.  Defining routes inside _run_local_server()
#    after the server has started means FastAPI never sees them — they silently 404.
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Liveness probe — also used by the frontend to wait for backend readiness."""
    collector = HyperliquidCollector._instance
    if collector is None:
        # Process is alive but collector hasn't initialised yet — still healthy
        return {"status": "starting", "uptime": 0}
    return {"status": "ok", "uptime": time.time() - collector.started_at}


@app.get("/state")
async def get_state():
    """
    HTTP fallback for full state hydration.
    The frontend calls this immediately on page load so the dashboard is populated
    even if the WebSocket connection is delayed or fails (e.g. on hard refresh while
    the proxy is still warming up).
    """
    from fastapi.responses import JSONResponse
    collector = HyperliquidCollector._instance
    if collector is None:
        return JSONResponse({"error": "collector_not_ready"}, status_code=503)
    return JSONResponse(collector.get_state(), headers=_NO_CACHE)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Live push channel — sends a full_state snapshot on connect, then streams
    incremental updates.  Defined at module level so the route is always compiled
    into the ASGI app before uvicorn starts.
    """
    collector = HyperliquidCollector._instance
    if collector is None:
        await websocket.close(code=1013)  # Try again later
        return

    await websocket.accept()
    collector.local_clients.add(websocket)
    logger.info("New dashboard client connected via FastAPI WS")
    try:
        with collector._data_lock:
            snapshot = collector._get_full_state_snapshot()
            await websocket.send_text(json.dumps({
                'channel': 'full_state',
                'data': snapshot
            }))

            display_names = {
                'HL': 'Hyperliquid Future', 'BIN': 'Binance Spot/Future',
                'BYB': 'Bybit Spot/Future', 'OKX': 'OKX Spot/Future',
                'KRK': 'Kraken Spot/Future', 'CB': 'Coinbase Spot/Future',
                'DRB': 'Deribit Future', 'BFX': 'Bitfinex Spot',
                'BGT': 'Bitget Spot/Future', 'MEXC': 'MEXC Spot',
                'UPB': 'Upbit Spot', 'GATE': 'Gate.io Spot/Future',
            }
            status_log = [
                f"{display_names.get(ex, ex)}: {'Connected' if collector.exchange_status[ex]['connected'] else 'Disconnected'}"
                for ex in collector.exchanges
            ]
            logger.info(f"Dashboard Refresh — Exchange Status: {', '.join(status_log)}")

        while True:
            message = await websocket.receive_text()
            try:
                data = json.loads(message)
                if data.get('method') == 'ping':
                    await websocket.send_text(json.dumps({'channel': 'pong'}))
                elif data.get('method') == 'clear_current':
                    with collector._data_lock:
                        for c in collector.coins:
                            collector.data[c]['current_buy_vol'] = 0.0
                            collector.data[c]['current_sell_vol'] = 0.0
                            collector.data[c]['current_buy_count'] = 0
                            collector.data[c]['current_sell_count'] = 0
                            collector.data[c]['current_since'] = time.time() * 1000
                        logger.info("Current accumulation cleared by user")
                    # Save OUTSIDE the lock — _save_snapshot -> get_state re-acquires it
                    collector._save_snapshot()
                elif data.get('method') == 'set_threshold':
                    coin = data.get('coin')
                    val = data.get('value')
                    if coin and val:
                        with collector._data_lock:
                            collector.whale_thresholds[coin] = float(val)
                        logger.info(f"Threshold for {coin} set to ${val}")
            except Exception as e:
                logger.error(f"WS Message Error: {e}")
    except WebSocketDisconnect:
        logger.info("Dashboard client disconnected")
    except Exception as e:
        logger.error(f"WS Error: {e}")
    finally:
        collector.local_clients.discard(websocket)

class HyperliquidCollector:
    """Singleton background collector."""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    # Handle Streamlit hot-reloads causing zombie threads
                    old_inst = getattr(sys, '_whaleflow_collector', None)
                    if old_inst is not None and getattr(old_inst, 'running', False):
                        try:
                            old_inst.shutdown()
                            time.sleep(2)  # Give old websockets time to release port
                        except Exception as e:
                            logger.error(f"Failed to shutdown old instance: {e}")
                            
                    inst = cls()
                    cls._instance = inst
                    sys._whaleflow_collector = inst
                    inst.start()
                    _server_ready.set()  # Ungate uvicorn — collector is ready
        return cls._instance

    def __init__(self):
        self.coins = CONFIG['coins']
        self.whale_thresholds = CONFIG['whale_thresholds']
        self.mega_thresholds = CONFIG['mega_thresholds']

        self.data: Dict[str, CoinData] = {}
        self._data_lock = threading.Lock()
        self.running = False
        self.connected = False
        self.started_at: Optional[float] = None

        # Exchange health tracking
        self.exchanges = [
            'HL', 'BIN', 'BYB', 'OKX', 'KRK', 'CB', 
            'DRB', 'BFX', 'BGT', 'MEXC', 'UPB', 'GATE'
        ]
        self.exchange_status = {
            ex: {'connected': False, 'last_msg': 0.0, 'last_error': ''}
            for ex in self.exchanges
        }

        # Freshness timestamps
        self.last_funding_update = 0
        self.last_trade_update = 0
        self.snapshot_loaded = False

        # Per-coin EMA state for funding smoothing (alpha=0.15 ≈ 12-period EMA)
        # A slower alpha means stronger smoothing — equivalent to a high-TF indicator.
        self._funding_ema: Dict[str, float] = {coin: 0.0 for coin in CONFIG['coins']}
        self._funding_ema_alpha = 0.15

        # Trade deduplication
        self._dedupe_lock = threading.Lock()
        self._dedupe_set = set()
        self._dedupe_queue = deque()
        
        # Currency Conversion
        self.krw_usd_rate = 1380.0

        self.local_clients = set()
        self.local_loop: Optional[asyncio.AbstractEventLoop] = None

        for coin in self.coins:
            self.data[coin] = self._new_coin_data()

        # Log throttling
        self._last_warn_time = {}

        # Trade aggregation buffer for detecting whales in fragmented fills
        self._agg_lock = threading.Lock()
        self._agg_buffer: Dict[tuple, deque] = {}
        self._agg_entry_count = 0

        # Load previous snapshot
        self._load_snapshot()

    def _new_coin_data(self) -> CoinData:
        return {
            'whale_trades': deque(maxlen=2000),
            'total_buy_vol': 0.0,
            'total_sell_vol': 0.0,
            'buy_count': 0,
            'sell_count': 0,
            'current_buy_vol': 0.0,
            'current_sell_vol': 0.0,
            'current_buy_count': 0,
            'current_sell_count': 0,
            'whale_buckets': deque(maxlen=1440),
            'mega_whales': deque(maxlen=500),
            'abs_cum_buy': 0.0,
            'abs_cum_sell': 0.0,
            'abs_snapshots': deque(maxlen=int(cast(Any, CONFIG['abs_snapshots_maxlen']))),
            'abs_detected': False,
            'abs_side': None,
            'abs_conditions': {'flow': False, 'reversal': False, 'oi': False, 'funding': False},
            'abs_conditions_met': 0,
            'abs_metrics': {'cvd': 0, 'vol': 0, 'price_delta': 0, 'oi_delta': 0, 'imbalance': 50, 'funding': 0},
            'last_trade_price': 0.0,
            'funding': 0.0,
            'funding_history': deque(maxlen=int(cast(Any, CONFIG['funding_history_maxlen']))),
            'market_history': deque(maxlen=int(cast(Any, CONFIG['market_history_maxlen']))),
            'mark_px': 0.0,
            'oracle_px': 0.0,
            'open_interest': 0.0,
            'day_volume': 0.0,
            'pressure_history': deque(maxlen=int(cast(Any, CONFIG['pressure_history_maxlen']))),
            'last_pressure_snap': {'buys': 0.0, 'sells': 0.0},
            'volume_buckets': deque(maxlen=int(cast(Any, CONFIG['volume_buckets_maxlen']))),
            'current_bucket_buy': 0.0,
            'current_bucket_sell': 0.0,
            'current_since': time.time() * 1000,
            'signals': {
                'absorption':      {'active': False, 'side': None, 'detail': '', 'time': 0.0},
                'cvd_divergence':  {'active': False, 'side': None, 'detail': '', 'time': 0.0},
                'oi_divergence':   {'active': False, 'side': None, 'detail': '', 'time': 0.0},
                'volume_climax':   {'active': False, 'side': None, 'detail': '', 'time': 0.0},
                'funding_extreme': {'active': False, 'side': None, 'detail': '', 'time': 0.0},
                'initiative':      {'active': False, 'side': None, 'detail': '', 'time': 0.0},
                'clustering':      {'active': False, 'side': None, 'detail': '', 'time': 0.0},
            },
            'alert_level': 0,
            'alert_label': 'Quiet',
            'regime': {
                'score': 50,
                'label': 'ANALYZING...',
                'css_class': '',
                'last_change_time': 0.0,
                'price_history': [],
                'range_score': 0,
                'volume_score': 0,
                'cvd_score': 0,
                'balance_score': 0,
            },
        }

    # ==================== LIFECYCLE ====================

    def start(self):
        self.running = True
        self.started_at = time.time()
        self.local_loop = asyncio.new_event_loop() # Create loop for broadcasting
        self.enable_local_server = os.environ.get('WHALEFLOW_ENABLE_LOCAL_SERVER', '1').lower() not in {'0', 'false', 'no'}
        
        def run_loop():
            asyncio.set_event_loop(self.local_loop)
            self.local_loop.run_forever()
            
        threading.Thread(target=run_loop, daemon=True).start()
        
        # Rest of the threads
        threading.Thread(target=self._run_ws, daemon=True).start()
        threading.Thread(target=self._run_ws_coinbase, daemon=True).start()
        # threading.Thread(target=self._run_ws_bitget, daemon=True).start() # Handled below
        threading.Thread(target=self._run_ws_bitfinex, daemon=True).start()
        threading.Thread(target=self._run_ws_binance, daemon=True).start()
        threading.Thread(target=self._run_ws_bybit, daemon=True).start()
        threading.Thread(target=self._run_ws_okx, daemon=True).start()
        threading.Thread(target=self._run_ws_kraken, daemon=True).start()
        threading.Thread(target=self._run_ws_bitget, daemon=True).start()
        threading.Thread(target=self._run_ws_deribit, daemon=True).start()
        threading.Thread(target=self._run_ws_mexc, daemon=True).start()
        threading.Thread(target=self._run_ws_gate, daemon=True).start()
        threading.Thread(target=self._run_ws_upbit, daemon=True).start()
        threading.Thread(target=self._run_funding, daemon=True).start()
        threading.Thread(target=self._run_signals, daemon=True).start()
        threading.Thread(target=self._run_snapshots, daemon=True).start()
        threading.Thread(target=self._run_pressure, daemon=True).start()
        threading.Thread(target=self._run_persist, daemon=True).start()
        threading.Thread(target=self._run_aggregator, daemon=True).start()
        
        # Streamlit mode disables the embedded FastAPI server and only uses state injection.
        if self.enable_local_server:
            threading.Thread(target=self._run_local_server, daemon=True).start()
        else:
            logger.info('Embedded FastAPI server disabled for Streamlit mode')
        
        logger.info("âœ… WhaleFlow Collector System Started")
        
        # Wire live log broadcasting through the local WS
        def _broadcast_log(entry):
            if self.local_loop and self.local_clients:
                msg = json.dumps({'channel': 'log', 'data': entry})
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_local(msg), self.local_loop
                )
        _mh.set_broadcaster(_broadcast_log)

    def shutdown(self):
        if not self.running:
            return
        logger.info("Shutting down collector...")
        self.running = False
        self._save_snapshot()
        logger.info("Collector shutdown complete.")

    def _run_persist(self):
        """Dedicated thread: saves snapshot every 10s independently of the pipeline."""
        time.sleep(20)  # Let collector warm up first
        while self.running:
            try:
                self._save_snapshot()
            except Exception as e:
                logger.error(f"Persist error: {e}")
            time.sleep(10)

    # ==================== SNAPSHOT PERSISTENCE ====================

    def _save_snapshot(self):
        try:
            state = self.get_state()
            snap_dir = os.path.dirname(CONFIG['snapshot_path'])
            if snap_dir:
                os.makedirs(snap_dir, exist_ok=True)
            tmp_path = CONFIG['snapshot_path'] + '.tmp'
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, default=str)  # default=str prevents crashes on non-serializable types
            os.replace(tmp_path, CONFIG['snapshot_path'])
            logger.info(f"Snapshot saved to {CONFIG['snapshot_path']}")
        except Exception as e:
            logger.exception(f"Failed to save snapshot ({type(e).__name__}: {e})")

    def _load_snapshot(self):
        path = CONFIG['snapshot_path']
        if not os.path.exists(path):
            logger.info("No snapshot found, starting fresh")
            return
        try:
            with open(path, 'r', encoding='utf-8') as f:
                state = json.load(f)
            if not state or 'coins' not in state:
                logger.warning("Snapshot file is empty or invalid")
                return

            # Restore system-level state (don't overwrite new exchange keys)
            self.started_at = state.get('started_at', self.started_at)
            self.last_funding_update = state.get('last_funding_update', self.last_funding_update)
            self.last_trade_update = state.get('last_trade_update', self.last_trade_update)
            # We skip 'exchange_status' here to ensure system health is live and not loaded from hours ago
            # which causes "ghost" green dots or stale latency.

            
            if 'whale_thresholds' in state:
                self.whale_thresholds.update(state['whale_thresholds'])
            
            # Skip restoring log_buffer — old errors/warnings should not reappear after restart

            with self._data_lock:
                for coin in self.coins:
                    sc = state['coins'].get(coin)
                    if not sc:
                        continue
                    d = self.data[coin]

                    for key in ['total_buy_vol', 'total_sell_vol', 'buy_count', 'sell_count',
                                'current_buy_vol', 'current_sell_vol', 'current_buy_count', 'current_sell_count',
                                'funding', 'mark_px', 'oracle_px', 'open_interest', 'day_volume', 'last_trade_price',
                                'current_since']:
                        if key in sc:
                            d[key] = sc[key]

                    if sc.get('whale_trades'):
                        d['whale_trades'] = deque(sc['whale_trades'][:2000], maxlen=2000)
                    if sc.get('mega_whales'):
                        d['mega_whales'] = deque(sc['mega_whales'][:500], maxlen=500)
                    if sc.get('whale_buckets'):
                        d['whale_buckets'] = deque(sc['whale_buckets'][-1440:], maxlen=1440)
                    if sc.get('pressure_history'):
                        d['pressure_history'] = deque(sc['pressure_history'][-CONFIG['pressure_history_maxlen']:], maxlen=CONFIG['pressure_history_maxlen'])
                    if sc.get('volume_buckets'):
                        d['volume_buckets'] = deque(sc['volume_buckets'][-CONFIG['volume_buckets_maxlen']:], maxlen=CONFIG['volume_buckets_maxlen'])
                    if sc.get('funding_history'):
                        d['funding_history'] = deque(
                            sc['funding_history'][-CONFIG['funding_history_maxlen']:],
                            maxlen=CONFIG['funding_history_maxlen']
                        )
                    if sc.get('market_history'):
                        d['market_history'] = deque(
                            sc['market_history'][-CONFIG['market_history_maxlen']:],
                            maxlen=CONFIG['market_history_maxlen']
                        )

                    if sc.get('abs'):
                        sa = sc['abs']
                        d['abs_cum_buy'] = sa.get('cum_buy', 0.0)
                        d['abs_cum_sell'] = sa.get('cum_sell', 0.0)
                        d['abs_detected'] = sa.get('detected', False)
                        d['abs_side'] = sa.get('side')
                        d['abs_conditions'] = sa.get('conditions', d['abs_conditions'])
                        d['abs_conditions_met'] = sa.get('conditions_met', 0)
                        d['abs_metrics'] = sa.get('metrics', d['abs_metrics'])
                        if sa.get('snapshots'):
                            d['abs_snapshots'] = deque(
                                sa['snapshots'][-CONFIG['abs_snapshots_maxlen']:],
                                maxlen=CONFIG['abs_snapshots_maxlen']
                            )

                    if sc.get('signals'):
                        d['signals'] = sc['signals']
                    d['alert_level'] = sc.get('alert_level', 0)
                    d['alert_label'] = sc.get('alert_label', 'Quiet')

                    if sc.get('regime'):
                        d['regime'] = sc['regime']

                    d['last_pressure_snap'] = {'buys': d['total_buy_vol'], 'sells': d['total_sell_vol']}

            self.snapshot_loaded = True
            logger.info(f"Snapshot loaded from {path}")
        except Exception:
            logger.exception(f"Failed to load snapshot from {path}")

    # ==================== WEBSOCKET - Hyperliquid ====================

    def _run_ws(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop())
            except Exception as e:
                logger.error(f"WS HL connection error: {e}")
                self.connected = False
                self.exchange_status['HL']['connected'] = False
                self.exchange_status['HL']['last_error'] = str(e)
                time.sleep(5)

    async def _ws_loop(self):
        async with websockets.connect('wss://api.hyperliquid.xyz/ws', ping_interval=20, ping_timeout=10) as ws:
            self.connected = True
            self.exchange_status['HL']['connected'] = True
            logger.info("[SUCCESS] Hyperliquid Future connected")

            for coin in self.coins:
                await ws.send(json.dumps({
                    'method': 'subscribe',
                    'subscription': {'type': 'trades', 'coin': coin}
                }))

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('channel') == 'trades' and 'data' in msg:
                        hw_trades = []
                        for t in msg['data']:
                            t['exchange'] = 'HL'
                            hw_trades.append(t)
                        self._process_trades(hw_trades)
                        self.exchange_status['HL']['last_msg'] = time.time()
                except Exception:
                    logger.warning("WS HL parse error", exc_info=True)

    # ==================== WEBSOCKET - Coinbase ====================

    def _run_ws_coinbase(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_coinbase())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['CB']['last_error'] = "Geo-Blocked"
                    logger.warning(f"Coinbase connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS Coinbase connection error: {e}")
                    self.exchange_status['CB']['last_error'] = str(e)
                self.exchange_status['CB']['connected'] = False
                time.sleep(30)

    async def _ws_loop_coinbase(self):
        url = "wss://advanced-trade-ws.coinbase.com"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['CB']['connected'] = True
            logger.info("[SUCCESS] Coinbase Spot + Future connected")
            # Subscribe Spot and International Perps
            symbols = [f"{coin}-USDT" for coin in self.coins]
            symbols += [f"{coin}-PERP" for coin in self.coins if coin in ['BTC', 'ETH', 'SOL', 'XRP']]
            await ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": symbols,
                "channel": "market_trades"
            }))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('channel') == 'market_trades' and 'events' in msg:
                        for event in msg['events']:
                            if event.get('type') != 'update' or 'trades' not in event:
                                continue
                            for t in event['trades']:
                                coin = t['product_id'].split('-')[0]
                                if coin not in self.coins:
                                    continue
                                dtime = datetime.fromisoformat(t['time'].replace('Z', '+00:00'))
                                self._process_trades([{
                                    'coin': coin, 'px': float(t['price']), 'sz': float(t['size']),
                                    'side': 'B' if t['side'] == 'BUY' else 'S',
                                    'time': int(dtime.timestamp() * 1000), 'exchange': 'CB'
                                }])
                                self.exchange_status['CB']['last_msg'] = time.time()
                except Exception:
                    logger.warning("WS Coinbase parse error", exc_info=True)

    # ==================== WEBSOCKET - Bitfinex ====================

    def _run_ws_bitfinex(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_bitfinex())
            except Exception as e:
                err_str = str(e).lower()
                is_geo = any(x in err_str for x in ["451", "forbidden", "rejected", "403"])
                if is_geo:
                    logger.warning("Bitfinex geo-blocked.")
                    self.exchange_status['BFX']['last_error'] = "Geo-Blocked"
                    self.exchange_status['BFX']['connected'] = False
                    break
                logger.error(f"WS Bitfinex error: {e}")
                self.exchange_status['BFX']['connected'] = False
                time.sleep(10)

    async def _ws_loop_bitfinex(self):
        url = "wss://api-pub.bitfinex.com/ws/2"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BFX']['connected'] = True
            logger.info("[SUCCESS] Bitfinex Spot connected")
            # Channel mapping
            chan_map = {}
            for coin in self.coins:
                # Subscribe Spot
                await ws.send(json.dumps({
                    "event": "subscribe", "channel": "trades", "symbol": f"t{coin}UST"
                }))
                # Subscribe Futures (where available)
                if coin in ['BTC', 'ETH', 'SOL']:
                    await ws.send(json.dumps({
                        "event": "subscribe", "channel": "trades", "symbol": f"t{coin}F0:UST0"
                    }))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if isinstance(msg, dict) and msg.get('event') == 'subscribed':
                        # Extract coin from symbols like tBTCUST, tETHF0:UST0
                        sym = msg['symbol']
                        if sym.startswith('t'):
                            sym = sym[1:]  # Remove leading 't'
                        # Remove futures/USDT suffixes
                        for suffix in ['F0:UST0', 'F0:USTF0', 'UST', 'USD']:
                            if suffix in sym:
                                sym = sym.split(suffix)[0]
                                break
                        chan_map[msg['chanId']] = sym
                    elif isinstance(msg, list) and len(msg) >= 3 and msg[1] in ['te', 'tu']:
                        chan_id = msg[0]
                        coin = chan_map.get(chan_id, '?')
                        t = msg[2]
                        # Bitfinex trade array: [ID, TIME, AMOUNT, PRICE]
                        # Positive amount = buy, Negative = sell
                        amt = float(t[2])
                        self._process_trades([{
                            'coin': coin, 'px': float(t[3]), 'sz': abs(amt),
                            'side': 'B' if amt > 0 else 'S',
                            'time': int(t[1]), 'exchange': 'BFX'
                        }])
                        self.exchange_status['BFX']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - Bitget ====================

    def _run_ws_bitget(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_bitget())
            except Exception as e:
                err_str = str(e).lower()
                self.exchange_status['BGT']['connected'] = False
                
                # Downgrade to warning and throttle
                is_geo = any(x in err_str for x in ["close frame", "rejected", "403", "extra_headers", "451"])
                now = time.time()
                last_log = self._last_warn_time.get('BGT', 0)
                
                if is_geo:
                    self.exchange_status['BGT']['last_error'] = "Geo-Blocked"
                    if now - last_log > 3600: # Log once per hour
                        logger.warning(f"Bitget connectivity: likely geo-blocked or rejected. ({e})")
                        self._last_warn_time['BGT'] = now
                else:
                    if now - last_log > 60: # Throttle rapid errors
                        logger.warning(f"WS Bitget connection issue: {e}")
                        self._last_warn_time['BGT'] = now
                time.sleep(30)

    async def _ws_loop_bitget(self):
        # Using alternate domain to try and bypass regional blocks
        url = "wss://ws.bitgetapi.com/v2/ws/public"
        # Removed extra_headers as some environments' websockets lib pass it incorrectly to lower levels
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BGT']['connected'] = True
            logger.info("[SUCCESS] Bitget Spot + Future connected")
            # Subscribe Spot and Futures
            args = []
            for coin in self.coins:
                args.append({"instType": "SPOT", "channel": "trade", "instId": f"{coin}USDT"})
                if coin in ['BTC', 'ETH', 'SOL', 'XRP']:
                    args.append({"instType": "USDT-FUTURES", "channel": "trade", "instId": f"{coin}USDT"})
            
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if 'data' in msg:
                        for t in msg['data']:
                            coin = t['instId'].replace('USDT', '')
                            inst_type = msg.get('arg', {}).get('instType', 'SPOT')
                            # Map side to 'B'/'S' for internal consistency
                            side = 'B' if t['side'].lower() == 'buy' else 'S'
                            self._process_trades([{
                                'coin': coin, 'px': float(t['price']), 'sz': float(t['size']),
                                'side': side,
                                'time': int(t['ts']), 'exchange': f'BGT_{inst_type}'
                            }])
                            self.exchange_status['BGT']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - Binance ====================

    def _run_ws_binance(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_binance())
            except Exception as e:
                self.exchange_status['BIN']['connected'] = False
                err_str = str(e).lower()
                now = time.time()
                last_log = self._last_warn_time.get('BIN', 0)
                
                if any(x in err_str for x in ["451", "rejected", "forbidden", "403"]):
                    self.exchange_status['BIN']['last_error'] = "Geo-Blocked"
                    if now - last_log > 3600:
                        logger.warning(f"Binance connectivity: likely geo-blocked or rejected. ({e})")
                        self._last_warn_time['BIN'] = now
                else:
                    if now - last_log > 60:
                        logger.warning(f"WS Binance connection issue: {e}")
                        self._last_warn_time['BIN'] = now
                time.sleep(30)

    async def _ws_loop_binance(self):
        async def spot_loop():
            # Using US-compliant public API for spot
            url = "wss://stream.binance.us:9443/ws"
            async with websockets.connect(url) as ws:
                self.exchange_status['BIN']['connected'] = True
                logger.info("[SUCCESS] Binance Spot connected")
                streams = [f"{c.lower()}usdt@aggTrade" for c in self.coins]
                payload = {"method": "SUBSCRIBE", "params": streams, "id": 1}
                await ws.send(json.dumps(payload))
                async for raw in ws:
                    msg = json.loads(raw)
                    if 's' in msg:
                        coin = msg['s'].replace('USDT', '')
                        self._process_trades([{
                            'coin': coin, 'px': float(msg['p']), 'sz': float(msg['q']),
                            'side': 'S' if msg['m'] else 'B', 'time': int(msg['T']), 'exchange': 'BIN_SPOT'
                        }])
                        self.exchange_status['BIN']['last_msg'] = time.time()

        async def futures_loop():
            # Using data-stream.binance.com which is often more accessible from US cloud environments
            url = "wss://data-stream.binance.com/ws"
            async with websockets.connect(url) as ws:
                logger.info("[SUCCESS] Binance Future connected")
                streams = [f"{c.lower()}usdt@aggTrade" for c in self.coins]
                payload = {"method": "SUBSCRIBE", "params": streams, "id": 1}
                await ws.send(json.dumps(payload))
                async for raw in ws:
                    msg = json.loads(raw)
                    if 's' in msg:
                        coin = msg['s'].replace('USDT', '')
                        self._process_trades([{
                            'coin': coin, 'px': float(msg['p']), 'sz': float(msg['q']),
                            'side': 'S' if msg['m'] else 'B', 'time': int(msg['T']), 'exchange': 'BIN_FUT'
                        }])
                        self.exchange_status['BIN']['last_msg'] = time.time()

        await asyncio.gather(spot_loop(), futures_loop())

    # ==================== WEBSOCKET - Bybit ====================

    def _run_ws_bybit(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_bybit())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['BYB']['last_error'] = "Geo-Blocked"
                    logger.warning(f"Bybit connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS Bybit error: {e}")
                self.exchange_status['BYB']['connected'] = False
                time.sleep(30)

    async def _ws_loop_bybit(self):
        async def linear_loop():
            url = "wss://stream.bybit.com/v5/public/linear"
            async with websockets.connect(url) as ws:
                self.exchange_status['BYB']['connected'] = True
                logger.info("[SUCCESS] Bybit Future connected")
                args = [f"publicTrade.{c}USDT" for c in self.coins]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                async for raw in ws:
                    msg = json.loads(raw)
                    if 'data' in msg:
                        for t in msg['data']:
                            coin = t['s'].replace('USDT', '')
                            self._process_trades([{
                                'coin': coin, 'px': float(t['p']), 'sz': float(t['v']),
                                'side': 'B' if t['S'] == 'Buy' else 'S', 'time': int(t['T']), 'exchange': 'BYB_FUT'
                            }])
                            self.exchange_status['BYB']['last_msg'] = time.time()

        async def spot_loop():
            url = "wss://stream.bybit.com/v5/public/spot"
            async with websockets.connect(url) as ws:
                logger.info("[SUCCESS] Bybit Spot connected")
                args = [f"publicTrade.{c}USDT" for c in self.coins]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                async for raw in ws:
                    msg = json.loads(raw)
                    if 'data' in msg:
                        for t in msg['data']:
                            coin = t['s'].replace('USDT', '')
                            self._process_trades([{
                                'coin': coin, 'px': float(t['p']), 'sz': float(t['v']),
                                'side': 'B' if t['S'] == 'Buy' else 'S', 'time': int(t['T']), 'exchange': 'BYB_SPOT'
                            }])
                            self.exchange_status['BYB']['last_msg'] = time.time()

        await asyncio.gather(linear_loop(), spot_loop())

    # ==================== WEBSOCKET - OKX ====================

    def _run_ws_okx(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_okx())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['OKX']['last_error'] = "Geo-Blocked"
                    logger.warning(f"OKX connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS OKX error: {e}")
                self.exchange_status['OKX']['connected'] = False
                time.sleep(30)

    async def _ws_loop_okx(self):
        url = "wss://ws.okx.com:8443/ws/v5/public"
        async with websockets.connect(url) as ws:
            self.exchange_status['OKX']['connected'] = True
            logger.info("[SUCCESS] OKX Spot + Future connected")
            args = []
            for c in self.coins:
                args.append({"channel": "trades", "instId": f"{c}-USDT"})
                args.append({"channel": "trades", "instId": f"{c}-USDT-SWAP"})
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            async for raw in ws:
                msg = json.loads(raw)
                if 'data' in msg:
                    for t in msg['data']:
                        coin = t['instId'].split('-')[0]
                        self._process_trades([{
                            'coin': coin, 'px': float(t['px']), 'sz': float(t['sz']),
                            'side': 'B' if t['side'] == 'buy' else 'S', 'time': int(t['ts']), 'exchange': 'OKX'
                        }])
                        self.exchange_status['OKX']['last_msg'] = time.time()

    # ==================== WEBSOCKET - Kraken ====================

    def _run_ws_kraken(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_kraken())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['KRK']['last_error'] = "Geo-Blocked"
                    logger.warning(f"Kraken connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS Kraken error: {e}")
                self.exchange_status['KRK']['connected'] = False
                time.sleep(30)

    async def _ws_loop_kraken(self):
        async def spot_loop():
            url = "wss://ws.kraken.com/v2"
            async with websockets.connect(url) as ws:
                self.exchange_status['KRK']['connected'] = True
                logger.info("[SUCCESS] Kraken Spot connected")
                pairs = [f"{c}/USD" for c in self.coins]
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {"channel": "trade", "symbol": pairs}
                }))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get('channel') == 'trade' and msg.get('type') == 'update':
                        for t in msg['data']:
                            coin = t['symbol'].split('/')[0]
                            self._process_trades([{
                                'coin': coin, 'px': float(t['price']), 'sz': float(t['qty']),
                                'side': 'B' if t['side'] == 'buy' else 'S', 
                                'time': int(datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00')).timestamp() * 1000), 
                                'exchange': 'KRK_SPOT'
                            }])
                            self.exchange_status['KRK']['last_msg'] = time.time()

        async def futures_loop():
            url = "wss://futures.kraken.com/ws/v1"
            async with websockets.connect(url) as ws:
                logger.info("[SUCCESS] Kraken Future connected")
                # Kraken Futures uses XBT instead of BTC
                kraken_coins = []
                for c in self.coins:
                    k_c = 'xbt' if c.upper() == 'BTC' else c.lower()
                    kraken_coins.append(k_c)
                
                products = [f"pi_{kc}usd" for kc in kraken_coins if kc in ['xbt', 'eth', 'sol', 'xrp']]
                await ws.send(json.dumps({"event": "subscribe", "feed": "trade", "product_ids": products}))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get('feed') == 'trade' and not msg.get('event'):
                        prod_id = msg['product_id'].split('_')[1].upper()
                        coin = 'BTC' if prod_id == 'XBTUSD' else prod_id.replace('USD','')
                        self._process_trades([{
                            'coin': coin, 'px': float(msg['price']), 'sz': float(msg['qty']),
                            'side': 'B' if msg['side'] == 'buy' else 'S', 'time': int(msg['time']), 'exchange': 'KRK_FUT'
                        }])
                        self.exchange_status['KRK']['last_msg'] = time.time()

        await asyncio.gather(spot_loop(), futures_loop())

    # ==================== TRADE PROCESSING ====================

    def _process_trades(self, trades):
        now_ms = int(time.time() * 1000)
        valid_trades = []

        for trade in trades:
            # --- Validation ---
            try:
                price = float(trade['px'])
                size = float(trade['sz'])
            except (ValueError, TypeError, KeyError):
                continue
            if price <= 0 or size <= 0:
                continue
            value = price * size
            if value > CONFIG['max_trade_value']:
                logger.warning(f"Skipping large trade: ${value:,.0f} on {trade.get('coin', '?')}")
                continue
            trade_time = trade.get('time', now_ms)
            if isinstance(trade_time, str):
                trade_time = int(trade_time)
            if abs(trade_time - now_ms) > CONFIG['trade_time_tolerance_ms']:
                trade_time = now_ms
            trade['time'] = trade_time

            # --- Deduplication ---
            exchange = trade.get('exchange', 'HL')
            coin = trade.get('coin', 'BTC')
            trade_key = (exchange, coin, trade_time, price, size, trade['side'])
            with self._dedupe_lock:
                if trade_key in self._dedupe_set:
                    continue
                self._dedupe_set.add(trade_key)
                self._dedupe_queue.append(trade_key)
                while len(self._dedupe_queue) > CONFIG['dedupe_cache_size']:
                    old = self._dedupe_queue.popleft()
                    self._dedupe_set.discard(old)

            valid_trades.append(trade)

        if not valid_trades:
            return

        # Broadcast validated trades to local WS clients
        if self.local_loop and self.local_clients:
            formatted = {'channel': 'trades', 'data': valid_trades}
            msg_str = json.dumps(formatted)
            asyncio.run_coroutine_threadsafe(
                self._broadcast_local(msg_str), self.local_loop
            )

        self.last_trade_update = time.time()

        # Collect sub-threshold trades INSIDE the lock, buffer OUTSIDE to avoid deadlock
        below_threshold = []

        with self._data_lock:
            for trade in valid_trades:
                coin = trade.get('coin', 'BTC')
                if coin not in self.data:
                    continue

                price = float(trade['px'])
                size = float(trade['sz'])
                value = price * size
                is_buy = trade['side'] == 'B'
                trade_time = trade.get('time', now_ms)
                exchange = trade.get('exchange', 'HL')

                d = self.data[coin]
                d['last_trade_price'] = price

                # Volume accumulator (all trades)
                if is_buy:
                    d['abs_cum_buy'] += value
                    d['current_bucket_buy'] += value
                else:
                    d['abs_cum_sell'] += value
                    d['current_bucket_sell'] += value

                # Whale tracking
                coin_threshold = self.whale_thresholds.get(coin, 50)
                if value >= coin_threshold:
                    # Timeframe Bucketing (1-minute resolution)
                    minute_ts = (trade_time // 60000) * 60000

                    if not d['whale_buckets']:
                        d['whale_buckets'].append({'time': minute_ts, 'buy': 0.0, 'sell': 0.0, 'buy_count': 0, 'sell_count': 0})

                    latest_bucket = d['whale_buckets'][-1]
                    if minute_ts > latest_bucket['time']:
                        d['whale_buckets'].append({'time': minute_ts, 'buy': 0.0, 'sell': 0.0, 'buy_count': 0, 'sell_count': 0})
                        latest_bucket = d['whale_buckets'][-1]

                    if minute_ts == latest_bucket['time']:
                        if is_buy:
                            latest_bucket['buy'] += value
                            latest_bucket['buy_count'] += 1
                        else:
                            latest_bucket['sell'] += value
                            latest_bucket['sell_count'] += 1

                    is_mega = False
                    mega_type = None

                    # Check Aggressive Initiative
                    mega_thresh = self.mega_thresholds.get(coin, 1000000)
                    if value >= mega_thresh:
                        is_mega = True
                        mega_type = 'initiative'
                        side = 'bullish' if is_buy else 'bearish'
                        d['signals']['initiative'] = {
                            'active': True, 'side': side,
                            'detail': f"{'BUY' if is_buy else 'SELL'} ${value/1e6:.2f}M @ {price:,.1f}",
                            'time': time.time(),
                        }
                        d['mega_whales'].appendleft({
                            'time': trade_time, 'side': 'BUY' if is_buy else 'SELL',
                            'price': price, 'size': size, 'value': value,
                            'coin': coin, 'mega_type': 'initiative', 'exchange': exchange
                        })

                    whale_entry = {
                        'time': trade_time, 'side': 'BUY' if is_buy else 'SELL',
                        'price': price, 'size': size, 'value': value, 'coin': coin,
                        'is_mega': is_mega, 'mega_type': mega_type, 'exchange': exchange
                    }
                    d['whale_trades'].appendleft(whale_entry)

                    if is_buy:
                        d['total_buy_vol'] += value
                        d['buy_count'] += 1
                        d['current_buy_vol'] += value
                        d['current_buy_count'] += 1
                    else:
                        d['total_sell_vol'] += value
                        d['sell_count'] += 1
                        d['current_sell_vol'] += value
                        d['current_sell_count'] += 1

                    self._check_clustering(coin)
                elif value >= CONFIG['agg_min_value']:
                    # Below whale threshold but meaningful — buffer for aggregation
                    below_threshold.append((exchange, coin, trade['side'], trade_time, price, size, value))

        # Buffer sub-threshold trades OUTSIDE _data_lock (avoids deadlock with _agg_lock)
        for args in below_threshold:
            self._agg_add(*args)

    def _check_clustering(self, coin):
        """5+ same-side whale trades within 60 seconds = clustering."""
        d = self.data[coin]
        now_ms = time.time() * 1000
        cutoff = now_ms - CONFIG['clustering_window_ms']
        min_trades = CONFIG['clustering_min_trades']

        recent = [t for t in list(d['whale_trades'])[:30] if t['time'] >= cutoff]
        if len(recent) < min_trades:
            return

        buys = [t for t in recent if t['side'] == 'BUY']
        sells = [t for t in recent if t['side'] == 'SELL']

        cluster_side = None
        cluster_trades = []
        if len(buys) >= min_trades:
            cluster_side = 'bullish'
            cluster_trades = buys
        elif len(sells) >= min_trades:
            cluster_side = 'bearish'
            cluster_trades = sells

        if cluster_side:
            total_val = sum(t['value'] for t in cluster_trades)
            count = len(cluster_trades)
            side_str = 'BUY' if cluster_side == 'bullish' else 'SELL'
            d['signals']['clustering'] = {
                'active': True, 'side': cluster_side,
                'detail': f"{count} {side_str} trades (${total_val/1e6:.2f}M) in 60s",
                'time': time.time(),
            }
            last_mega = d['mega_whales'][0] if d['mega_whales'] else None
            if not last_mega or last_mega.get('mega_type') != 'clustering' or (time.time() * 1000 - last_mega.get('time', 0)) > CONFIG['clustering_window_ms']:
                d['mega_whales'].appendleft({
                    'time': int(time.time() * 1000), 'side': side_str,
                    'price': cluster_trades[0]['price'],
                    'size': sum(t['size'] for t in cluster_trades),
                    'value': total_val, 'coin': coin,
                    'mega_type': 'clustering', 'cluster_count': count, 'exchange': 'MIX'
                })

    # ==================== TRADE AGGREGATION ENGINE ====================

    def _agg_add(self, exchange, coin, side, time_ms, price, size, value):
        """Buffer a sub-threshold trade for aggregation detection."""
        key = (exchange, coin, side)
        with self._agg_lock:
            if self._agg_entry_count >= CONFIG['agg_max_entries']:
                return  # Safety cap — silently drop to prevent memory growth
            if key not in self._agg_buffer:
                self._agg_buffer[key] = deque()
            self._agg_buffer[key].append((time_ms, price, size, value))
            self._agg_entry_count += 1

    def _run_aggregator(self):
        """Daemon thread: flush aggregation buffer periodically to detect hidden whale activity."""
        time.sleep(10)  # Let collector warm up
        while self.running:
            try:
                self._flush_aggregation()
            except Exception as e:
                logger.error(f"Aggregator flush error: {e}")
            time.sleep(CONFIG['agg_flush_interval'])

    def _flush_aggregation(self):
        """Drain expired buffer entries and create whale trades for groups crossing threshold."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - CONFIG['agg_window_ms']

        # --- Phase 1: Drain expired entries from buffer (under _agg_lock only) ---
        to_process = {}
        with self._agg_lock:
            keys_to_delete = []
            for key, entries in self._agg_buffer.items():
                expired = []
                while entries and entries[0][0] <= cutoff:
                    expired.append(entries.popleft())
                    self._agg_entry_count -= 1
                if expired:
                    to_process[key] = expired
                if not entries:
                    keys_to_delete.append(key)
            for k in keys_to_delete:
                del self._agg_buffer[k]

        if not to_process:
            return

        # --- Phase 2: Evaluate aggregated groups (under _data_lock) ---
        whale_broadcasts = []
        with self._data_lock:
            for (exchange, coin, side), entries in to_process.items():
                if coin not in self.data:
                    continue

                total_value = sum(e[3] for e in entries)
                coin_threshold = self.whale_thresholds.get(coin, 50)

                if total_value < coin_threshold:
                    continue  # Combined value still below threshold — discard

                # --- Aggregated whale detected ---
                total_size = sum(e[2] for e in entries)
                vwap = total_value / total_size if total_size > 0 else entries[-1][1]
                latest_time = max(e[0] for e in entries)
                is_buy = side == 'B'
                fill_count = len(entries)
                d = self.data[coin]

                # Whale bucket tracking (1-minute resolution)
                minute_ts = (latest_time // 60000) * 60000
                if not d['whale_buckets']:
                    d['whale_buckets'].append({'time': minute_ts, 'buy': 0.0, 'sell': 0.0, 'buy_count': 0, 'sell_count': 0})
                latest_bucket = d['whale_buckets'][-1]
                if minute_ts > latest_bucket['time']:
                    d['whale_buckets'].append({'time': minute_ts, 'buy': 0.0, 'sell': 0.0, 'buy_count': 0, 'sell_count': 0})
                    latest_bucket = d['whale_buckets'][-1]
                if minute_ts == latest_bucket['time']:
                    if is_buy:
                        latest_bucket['buy'] += total_value
                        latest_bucket['buy_count'] += 1
                    else:
                        latest_bucket['sell'] += total_value
                        latest_bucket['sell_count'] += 1

                # Mega whale initiative check
                is_mega = False
                mega_type = None
                mega_thresh = self.mega_thresholds.get(coin, 1000000)
                if total_value >= mega_thresh:
                    is_mega = True
                    mega_type = 'initiative'
                    side_label = 'bullish' if is_buy else 'bearish'
                    d['signals']['initiative'] = {
                        'active': True, 'side': side_label,
                        'detail': f"AGG {'BUY' if is_buy else 'SELL'} ${total_value/1e6:.2f}M ({fill_count} fills) @ {vwap:,.1f}",
                        'time': time.time(),
                    }
                    d['mega_whales'].appendleft({
                        'time': latest_time, 'side': 'BUY' if is_buy else 'SELL',
                        'price': vwap, 'size': total_size, 'value': total_value,
                        'coin': coin, 'mega_type': 'initiative', 'exchange': exchange
                    })

                whale_entry = {
                    'time': latest_time, 'side': 'BUY' if is_buy else 'SELL',
                    'price': vwap, 'size': total_size, 'value': total_value, 'coin': coin,
                    'is_mega': is_mega, 'mega_type': mega_type,
                    'exchange': exchange, 'agg': fill_count,
                }
                d['whale_trades'].appendleft(whale_entry)
                whale_broadcasts.append(whale_entry)

                if is_buy:
                    d['total_buy_vol'] += total_value
                    d['buy_count'] += 1
                    d['current_buy_vol'] += total_value
                    d['current_buy_count'] += 1
                else:
                    d['total_sell_vol'] += total_value
                    d['sell_count'] += 1
                    d['current_sell_vol'] += total_value
                    d['current_sell_count'] += 1

                self._check_clustering(coin)

                logger.info(
                    f"⚡ Aggregated whale: {coin} {'BUY' if is_buy else 'SELL'} "
                    f"${total_value:,.0f} ({fill_count} fills) on {exchange} VWAP ${vwap:,.2f}"
                )

        # --- Phase 3: Broadcast outside locks ---
        if whale_broadcasts and self.local_loop and self.local_clients:
            try:
                msg = json.dumps({'channel': 'trades', 'data': whale_broadcasts}, default=str)
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_local(msg), self.local_loop
                )
            except Exception:
                pass  # Non-critical — don't let broadcast failure crash aggregator

    # ==================== FUNDING / OI POLLER ====================

    def _run_funding(self):
        while self.running:
            try:
                self._fetch_funding()
            except Exception as e:
                logger.error(f"Funding poll error: {e}")
            time.sleep(CONFIG['funding_poll_interval'])

    def _fetch_funding(self):
        try:
            r = requests.post('https://api.hyperliquid.xyz/info',
                              json={'type': 'metaAndAssetCtxs'}, timeout=10)
            data = r.json()
            # Periodically refresh KRW rate
            krw_r = requests.get('https://api.exchangerate-api.com/v4/latest/USD', timeout=5)
            self.krw_usd_rate = krw_r.json().get('rates', {}).get('KRW', 1380.0)
        except Exception as e:
            logger.warning(f"Metadata/KRW API request failed: {e}")
            return
            
        if not data or not isinstance(data, list) or len(data) < 2:
            logger.warning(f"Unexpected funding data format received: {data}")
            return
            
        if not data[0] or not isinstance(data[0], dict) or 'universe' not in data[0]:
            logger.warning("Missing 'universe' key in funding data.")
            return

        universe = data[0]['universe']
        contexts = data[1] if isinstance(data[1], list) else []

        alpha = self._funding_ema_alpha
        with self._data_lock:
            for coin in self.coins:
                idx = next((i for i, u in enumerate(universe) if u['name'] == coin), -1)
                if idx == -1 or not contexts[idx]:
                    continue
                ctx = contexts[idx]
                d = self.data[coin]

                raw_funding = float(ctx.get('funding', '0'))

                # Apply EMA smoothing so UI shows a stable high-TF signal.
                # Bootstrap the EMA with the first real value received.
                prev_ema = self._funding_ema.get(coin, 0.0)
                if prev_ema == 0.0 and raw_funding != 0.0:
                    # First real sample — seed EMA directly
                    smoothed = raw_funding
                else:
                    smoothed = alpha * raw_funding + (1.0 - alpha) * prev_ema
                self._funding_ema[coin] = smoothed

                # Store smoothed value; history records raw for change-delta accuracy
                d['funding'] = smoothed
                d['mark_px'] = float(ctx.get('markPx', '0'))
                d['oracle_px'] = float(ctx.get('oraclePx', '0'))
                d['open_interest'] = float(ctx.get('openInterest', '0'))
                d['day_volume'] = float(ctx.get('dayNtlVlm', '0'))
                # Record RAW funding in history so 1h/4h/24h deltas reflect real changes
                d['funding_history'].append({'time': time.time(), 'funding': raw_funding})
                d['market_history'].append({
                    'time': time.time(),
                    'mark_px': d['mark_px'],
                    'open_interest': d['open_interest'] * d['mark_px'],
                    'day_volume': d['day_volume'],
                })
                self._evaluate_regime(coin, d)
        self.last_funding_update = time.time()

        if self.local_loop and self.local_clients:
            funding_payload = {
                'channel': 'funding',
                'data': {
                    'timestamp': self.last_funding_update,
                    'coins': {
                        coin: {
                            'funding': self.data[coin]['funding'],
                            'open_interest': self.data[coin]['open_interest'],
                            'mark_px': self.data[coin]['mark_px'],
                            'oracle_px': self.data[coin]['oracle_px'],
                            'day_volume': self.data[coin]['day_volume'],
                        } for coin in self.coins
                    }
                }
            }
            asyncio.run_coroutine_threadsafe(
                self._broadcast_local(json.dumps(funding_payload)), self.local_loop
            )

    # ==================== MARKET REGIME EVALUATOR ====================

    def _evaluate_regime(self, coin, d):
        rg = d['regime']
        now = time.time()
        mark_px = d.get('mark_px', 0)
        if mark_px <= 0:
            return

        rg['price_history'].append({'time': now, 'price': mark_px})
        cutoff = now - 3600
        rg['price_history'] = [p for p in rg['price_history'] if p['time'] > cutoff]

        if len(rg['price_history']) < 2:
            return

        # Factor 1: Price Range (0-40 pts)
        thirty_min_ago = now - 1800
        recent_prices = [p['price'] for p in rg['price_history'] if p['time'] > thirty_min_ago]
        if not recent_prices:
            return
        min_px = min(recent_prices)
        max_px = max(recent_prices)
        range_pct = ((max_px - min_px) / min_px * 100) if min_px > 0 else 0

        flat_range = CONFIG['regime_thresholds'].get(coin, 0.20)
        trend_range = flat_range * 2
        if range_pct >= trend_range:
            range_score = 40
        elif range_pct >= flat_range:
            range_score = round(15 + (range_pct - flat_range) / (trend_range - flat_range) * 25)
        else:
            range_score = round((range_pct / flat_range) * 15) if flat_range > 0 else 0
        rg['range_score'] = range_score

        # Factor 2: Volume vs Average (0-30 pts)
        buckets = list(d.get('volume_buckets', []))
        volume_score = 15
        if len(buckets) >= 3:
            avg_vol = sum(b.get('total', 0) for b in buckets) / len(buckets)
            current_vol = d.get('current_bucket_buy', 0) + d.get('current_bucket_sell', 0)
            if avg_vol > 0:
                ratio = current_vol / avg_vol
                if ratio >= 2.0:
                    volume_score = 30
                elif ratio >= 1.0:
                    volume_score = round(15 + (ratio - 1) * 15)
                elif ratio >= 0.4:
                    volume_score = round((ratio - 0.4) / 0.6 * 15)
                else:
                    volume_score = 0
        rg['volume_score'] = volume_score

        # Factor 3: CVD direction (0-15 pts)
        cvd_score = 7
        total_buy = d.get('total_buy_vol', 0)
        total_sell = d.get('total_sell_vol', 0)
        if total_buy + total_sell > 0:
            cvd = total_buy - total_sell
            total_vol = total_buy + total_sell
            cvd_ratio = abs(cvd) / total_vol
            if cvd_ratio >= 0.15:
                cvd_score = 15
            elif cvd_ratio >= 0.05:
                cvd_score = round(7 + (cvd_ratio - 0.05) / 0.10 * 8)
            else:
                cvd_score = round(cvd_ratio / 0.05 * 7)
        rg['cvd_score'] = cvd_score

        # Factor 4: Buy/Sell Balance (0-15 pts)
        balance_score = 7
        ph = list(d.get('pressure_history', []))
        if len(ph) >= 3:
            recent = ph[-6:]
            t_buys = sum(p.get('buys', 0) for p in recent)
            t_sells = sum(p.get('sells', 0) for p in recent)
            total = t_buys + t_sells
            if total > 0:
                buy_pct = t_buys / total
                imbalance = abs(buy_pct - 0.5) * 2
                if imbalance >= 0.3:
                    balance_score = 15
                elif imbalance >= 0.1:
                    balance_score = round(7 + (imbalance - 0.1) / 0.2 * 8)
                else:
                    balance_score = round(imbalance / 0.1 * 7)
        rg['balance_score'] = balance_score

        # Final Score
        raw_score = range_score + volume_score + cvd_score + balance_score

        # Hysteresis + Hold Time
        current_label = rg['label']
        hold_min = CONFIG['regime_hold_time']
        time_since = now - rg['last_change_time']
        can_change = rg['last_change_time'] == 0 or time_since >= hold_min

        if raw_score >= 60:
            new_label, new_class = '\U0001f7e2 TRENDING', 'trending'
        elif raw_score >= 30:
            new_label, new_class = '\U0001f7e1 CHOPPY', 'choppy'
        else:
            new_label, new_class = '\U0001f534 SIDEWAYS', 'sideways'

        if can_change:
            should_change = (
                (new_class == 'trending' and raw_score >= 65) or
                (new_class == 'sideways' and raw_score <= 25) or
                (new_class == 'choppy' and 35 <= raw_score <= 55) or
                'ANALYZING' in current_label
            )
            if should_change and new_label != current_label:
                rg['label'] = new_label
                rg['css_class'] = new_class
                rg['last_change_time'] = now

        rg['score'] = round(rg['score'] * 0.7 + raw_score * 0.3)

    # ==================== SNAPSHOT ENGINE (5 min) ====================

    def _run_snapshots(self):
        time.sleep(CONFIG['snapshot_initial_delay'])
        while self.running:
            try:
                self._take_snapshot()
                self._evaluate_absorption()
                self._save_snapshot()
                # Push fresh full state to all connected dashboard clients
                if self.local_loop and self.local_clients:
                    snapshot = self._get_full_state_snapshot()
                    msg = json.dumps({'channel': 'full_state', 'data': snapshot})
                    asyncio.run_coroutine_threadsafe(
                        self._broadcast_local(msg), self.local_loop
                    )
            except Exception as e:
                logger.error(f"Snapshot error: {e}")
            time.sleep(CONFIG['snapshot_interval'])

    def _take_snapshot(self):
        now = time.time()
        with self._data_lock:
            for coin in self.coins:
                d = self.data[coin]
                d['abs_snapshots'].append({
                    'time': now,
                    'cum_buy': d['abs_cum_buy'],
                    'cum_sell': d['abs_cum_sell'],
                    'price': d['last_trade_price'] or d['mark_px'],
                    'oi': d['open_interest'] * d['mark_px'],
                    'funding': d['funding'],
                })
                d['volume_buckets'].append({
                    'buy': d['current_bucket_buy'],
                    'sell': d['current_bucket_sell'],
                    'total': d['current_bucket_buy'] + d['current_bucket_sell'],
                })
                d['current_bucket_buy'] = 0.0
                d['current_bucket_sell'] = 0.0

        logger.info(f"Snapshot taken at {datetime.now().strftime('%H:%M:%S')}")

    def _evaluate_absorption(self):
        with self._data_lock:
            for coin in self.coins:
                d = self.data[coin]
                snaps = list(d['abs_snapshots'])

                if len(snaps) < 2:
                    d['abs_detected'] = False
                    d['abs_conditions_met'] = 0
                    d['abs_conditions'] = {'flow': False, 'reversal': False, 'oi': False, 'funding': False}
                    d['signals']['absorption'] = {'active': False, 'side': None, 'detail': ''}
                    continue

                oldest, newest = snaps[0], snaps[-1]
                buy_vol = newest['cum_buy'] - oldest['cum_buy']
                sell_vol = newest['cum_sell'] - oldest['cum_sell']
                total_vol = buy_vol + sell_vol
                cvd = buy_vol - sell_vol

                price_now = d['last_trade_price'] or d['mark_px']
                price_start = oldest['price'] or price_now
                price_delta = ((price_now - price_start) / price_start * 100) if price_start > 0 else 0

                oi_now = d['open_interest'] * d['mark_px']
                oi_start = oldest['oi'] or oi_now
                oi_delta = ((oi_now - oi_start) / oi_start * 100) if oi_start > 0 else 0

                funding = d['funding']
                imbalance = (max(buy_vol, sell_vol) / total_vol * 100) if total_vol > 0 else 50
                flow_is_buy = buy_vol > sell_vol

                d['abs_metrics'] = {
                    'cvd': cvd, 'vol': total_vol, 'price_delta': price_delta,
                    'oi_delta': oi_delta, 'imbalance': imbalance, 'funding': funding * 100,
                }

                c1 = imbalance >= CONFIG['absorption_imbalance_min'] and total_vol > CONFIG['absorption_vol_min']
                c2 = False
                if c1:
                    pdt = CONFIG['absorption_price_delta_max']
                    c2 = (flow_is_buy and price_delta <= pdt) or (not flow_is_buy and price_delta >= -pdt)
                c3 = oi_delta > CONFIG['absorption_oi_delta_min']
                ft = CONFIG['absorption_funding_threshold']
                c4 = (flow_is_buy and funding > ft) or (not flow_is_buy and funding < -ft)

                d['abs_conditions'] = {'flow': c1, 'reversal': c2, 'oi': c3, 'funding': c4}
                met = sum([c1, c2, c3, c4])
                d['abs_conditions_met'] = met
                d['abs_detected'] = (c1 and c2 and c3)
                d['abs_side'] = ('bearish' if flow_is_buy else 'bullish') if d['abs_detected'] else None

                d['signals']['absorption'] = {
                    'active': d['abs_detected'],
                    'side': d['abs_side'],
                    'detail': f"{'Sells' if d['abs_side'] == 'bullish' else 'Buys'} absorbed" if d['abs_detected'] else '',
                }

    # ==================== SIGNAL EVALUATOR (30s) ====================

    def _run_signals(self):
        time.sleep(CONFIG['signal_initial_delay'])
        while self.running:
            try:
                self._evaluate_all_signals()
            except Exception as e:
                logger.error(f"Signal eval error: {e}")
            time.sleep(CONFIG['signal_eval_interval'])

    def _evaluate_all_signals(self):
        now = time.time()
        with self._data_lock:
            for coin in self.coins:
                d = self.data[coin]
                sigs = d['signals']

                s_abs = sigs['absorption']['active']
                s_cvd = self._check_cvd_divergence(coin)
                s_oi = self._check_oi_divergence(coin)
                s_climax = self._check_volume_climax(coin)
                s_fund = self._check_funding_extreme(coin)

                s_init = False
                if sigs['initiative']['time'] > 0:
                    if now - sigs['initiative']['time'] < CONFIG['signal_decay_initiative']:
                        s_init = True
                    else:
                        sigs['initiative'] = {'active': False, 'side': None, 'detail': '', 'time': 0}

                s_clust = False
                if sigs['clustering']['time'] > 0:
                    if now - sigs['clustering']['time'] < CONFIG['signal_decay_clustering']:
                        s_clust = True
                    else:
                        sigs['clustering'] = {'active': False, 'side': None, 'detail': '', 'time': 0}

                active_count = sum([s_abs, bool(s_cvd), bool(s_oi), bool(s_climax), bool(s_fund)])
                d['alert_level'] = min(active_count, 4)

                if active_count == 0:
                    d['alert_label'] = 'Quiet'
                elif active_count == 1:
                    d['alert_label'] = 'Watch'
                elif active_count <= 3:
                    d['alert_label'] = 'High Probability'
                else:
                    d['alert_label'] = 'Extreme Conviction'

    def _check_cvd_divergence(self, coin):
        d = self.data[coin]
        snaps = list(d['abs_snapshots'])
        if len(snaps) < 4:
            return None

        mid = len(snaps) // 2
        s0, s1, s2, s3 = snaps[0], snaps[mid], snaps[mid], snaps[-1]
        p1s, p1e = s0['price'], s1['price']
        p2s, p2e = s2['price'], s3['price']
        if p1s <= 0 or p2s <= 0:
            return None

        pd1 = ((p1e - p1s) / p1s) * 100
        pd2 = ((p2e - p2s) / p2s) * 100
        cvd1 = (s1['cum_buy'] - s0['cum_buy']) - (s1['cum_sell'] - s0['cum_sell'])
        cvd2 = (s3['cum_buy'] - s2['cum_buy']) - (s3['cum_sell'] - s2['cum_sell'])

        pt = CONFIG['cvd_price_threshold']
        cr = CONFIG['cvd_ratio']
        result = None
        if pd1 > pt and pd2 >= 0 and cvd1 > 0 and cvd2 < cvd1 * cr:
            result = 'bearish'
            d['signals']['cvd_divergence'] = {
                'active': True, 'side': 'bearish',
                'detail': f"CVD fading: {cvd1/1e6:.1f}M -> {cvd2/1e6:.1f}M while price still up",
            }
        elif pd1 < -pt and pd2 <= 0 and cvd1 < 0 and abs(cvd2) < abs(cvd1) * cr:
            result = 'bullish'
            d['signals']['cvd_divergence'] = {
                'active': True, 'side': 'bullish',
                'detail': f"Sell pressure fading: {cvd1/1e6:.1f}M -> {cvd2/1e6:.1f}M while price still down",
            }
        else:
            d['signals']['cvd_divergence'] = {'active': False, 'side': None, 'detail': ''}
        return result

    def _check_oi_divergence(self, coin):
        d = self.data[coin]
        snaps = list(d['abs_snapshots'])
        if len(snaps) < 3:
            return None

        oldest, newest = snaps[0], snaps[-1]
        price_start = oldest['price']
        price_now = newest['price']
        oi_start = oldest['oi']
        oi_now = newest['oi']
        if price_start <= 0 or oi_start <= 0:
            return None

        price_delta = ((price_now - price_start) / price_start) * 100
        oi_delta = ((oi_now - oi_start) / oi_start) * 100
        t = CONFIG['oi_divergence_threshold']

        result = None
        if price_delta > t and oi_delta < -t:
            result = 'bearish'
            d['signals']['oi_divergence'] = {
                'active': True, 'side': 'bearish',
                'detail': f"Price +{price_delta:.2f}% but OI {oi_delta:.2f}% (distribution)",
            }
        elif price_delta < -t and oi_delta < -t:
            result = 'bullish'
            d['signals']['oi_divergence'] = {
                'active': True, 'side': 'bullish',
                'detail': f"Price {price_delta:.2f}% and OI {oi_delta:.2f}% (capitulation ending)",
            }
        else:
            d['signals']['oi_divergence'] = {'active': False, 'side': None, 'detail': ''}
        return result

    def _check_volume_climax(self, coin):
        d = self.data[coin]
        buckets = list(d['volume_buckets'])
        if len(buckets) < 3:
            return None

        avg = sum(b['total'] for b in buckets) / len(buckets)
        current = d['current_bucket_buy'] + d['current_bucket_sell']
        if avg <= 0:
            return None
        ratio = current / avg

        result = None
        if ratio >= CONFIG['volume_climax_ratio']:
            buy_pct = d['current_bucket_buy'] / current if current > 0 else 0.5
            if buy_pct > CONFIG['volume_climax_buy_high']:
                result = 'bearish'
                d['signals']['volume_climax'] = {
                    'active': True, 'side': 'bearish',
                    'detail': f"Buy volume {ratio:.1f}x avg Ã¢â‚¬â€ possible blow-off top",
                }
            elif buy_pct < CONFIG['volume_climax_buy_low']:
                result = 'bullish'
                d['signals']['volume_climax'] = {
                    'active': True, 'side': 'bullish',
                    'detail': f"Sell volume {ratio:.1f}x avg Ã¢â‚¬â€ possible capitulation bottom",
                }
            else:
                d['signals']['volume_climax'] = {'active': False, 'side': None, 'detail': ''}
        else:
            d['signals']['volume_climax'] = {'active': False, 'side': None, 'detail': ''}
        return result


    # ==================== WEBSOCKET - Deribit ====================

    def _run_ws_deribit(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_deribit())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['DRB']['last_error'] = "Geo-Blocked"
                    logger.warning(f"Deribit connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS Deribit error: {e}")
                self.exchange_status['DRB']['connected'] = False
                time.sleep(30)

    async def _ws_loop_deribit(self):
        url = "wss://www.deribit.com/ws/api/v2"
        async with websockets.connect(url) as ws:
            self.exchange_status['DRB']['connected'] = True
            logger.info("[SUCCESS] Deribit Future connected")
            channels = []
            for c in ['BTC', 'ETH', 'SOL']:
                if c in self.coins:
                    channels.append(f"trades.{c}-PERPETUAL.raw")
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "public/subscribe",
                "params": {"channels": channels}
            }))
            async for raw in ws:
                msg = json.loads(raw)
                if 'params' in msg and 'data' in msg['params']:
                    self.exchange_status['DRB']['last_msg'] = time.time()
                    for t in msg['params']['data']:
                        coin = t['instrument_name'].split('-')[0]
                        self._process_trades([{
                            'coin': coin, 'px': float(t['price']), 'sz': float(t['amount']),
                            'side': 'B' if t['direction'] == 'buy' else 'S',
                            'time': int(t['timestamp']), 'exchange': 'DRB'
                        }])

    # ==================== WEBSOCKET - MEXC ====================

    def _run_ws_mexc(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_mexc())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['MEXC']['last_error'] = "Geo-Blocked"
                    logger.warning(f"MEXC connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS MEXC error: {e}")
                self.exchange_status['MEXC']['connected'] = False
                time.sleep(30)

    async def _ws_loop_mexc(self):
        url = "wss://wbs.mexc.com/ws"
        async with websockets.connect(url) as ws:
            self.exchange_status['MEXC']['connected'] = True
            logger.info("[SUCCESS] MEXC Spot connected")
            params = [f"spot@public.deals.v3.api@{c}USDT" for c in self.coins]
            await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))
            async for raw in ws:
                msg = json.loads(raw)
                if 'd' in msg and 'deals' in msg['d']:
                    self.exchange_status['MEXC']['last_msg'] = time.time()
                    coin = msg['s'].replace('USDT', '')
                    trades = []
                    for t in msg['d']['deals']:
                        trades.append({
                            'coin': coin, 'px': float(t['p']), 'sz': float(t['q']),
                            'side': 'B' if t['S'] == 1 else 'S', 'time': int(t['t']),
                            'exchange': 'MEXC_SPOT'
                        })
                    self._process_trades(trades)

    # ==================== WEBSOCKET - Gate.io ====================

    def _run_ws_gate(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_gate())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['GATE']['last_error'] = "Geo-Blocked"
                    logger.warning(f"Gate.io connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS Gate error: {e}")
                self.exchange_status['GATE']['connected'] = False
                time.sleep(30)

    async def _ws_loop_gate(self):
        url = "wss://api.gateio.ws/ws/v4/"
        async with websockets.connect(url) as ws:
            self.exchange_status['GATE']['connected'] = True
            logger.info("[SUCCESS] Gate.io Spot connected")
            symbols = [f"{c}_USDT" for c in self.coins]
            await ws.send(json.dumps({
                "time": int(time.time()), "channel": "spot.trades",
                "event": "subscribe", "payload": symbols
            }))
            
            # Start a separate task for futures
            asyncio.create_task(self._gate_futures_listener())
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get('event') == 'update' and msg.get('channel') == 'spot.trades':
                    self.exchange_status['GATE']['last_msg'] = time.time()
                    data = msg.get('result')
                    coin = data.get('currency_pair', '').split('_')[0]
                    self._process_trades([{
                        'coin': coin, 'px': float(data['price']), 'sz': float(data['amount']),
                        'side': 'B' if data['side'] == 'buy' else 'S',
                        'time': int(float(data['create_time']) * 1000), 'exchange': 'GATE_SPOT'
                    }])

    async def _gate_futures_listener(self):
        url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                logger.info("[SUCCESS] Gate.io Future connected")
                symbols = [f"{c}_USDT" for c in self.coins]
                await ws.send(json.dumps({
                    "time": int(time.time()), "channel": "futures.trades",
                    "event": "subscribe", "payload": symbols
                }))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get('event') == 'update' and msg.get('channel') == 'futures.trades':
                            for t in msg['result']:
                                coin = t['contract'].split('_')[0]
                                self._process_trades([{
                                    'coin': coin, 'px': float(t['price']), 'sz': abs(float(t['size'])),
                                    'side': 'B' if t['size'] > 0 else 'S',
                                    'time': int(float(t['create_time']) * 1000), 'exchange': 'GATE_FUT'
                                }])
                                self.exchange_status['GATE']['last_msg'] = time.time()
                    except Exception: pass
        except Exception: pass

    def _run_ws_upbit(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_upbit())
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["451", "forbidden", "rejected", "403", "close frame"]):
                    self.exchange_status['UPB']['last_error'] = "Geo-Blocked"
                    logger.warning(f"Upbit connectivity: likely geo-blocked. ({e})")
                else:
                    logger.error(f"WS Upbit error: {e}")
                self.exchange_status['UPB']['connected'] = False
                time.sleep(30)

    async def _ws_loop_upbit(self):
        url = "wss://api.upbit.com/websocket/v1"
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                self.exchange_status['UPB']['connected'] = True
                logger.info("[SUCCESS] Upbit Spot connected")
                codes = [f"KRW-{c}" for c in self.coins]
                # Upbit Format: [{"ticket":"test"},{"type":"trade","codes":["KRW-BTC"]}]
                await ws.send(json.dumps([{"ticket": "whaleflow"}, {"type": "trade", "codes": codes}]))
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        if data.get('type') == 'trade':
                            coin = data['code'].split('-')[1]
                            side = 'B' if data['ask_bid'] == 'BID' else 'S'
                            # Convert KRW price to USD
                            px_usd = float(data['trade_price']) / self.krw_usd_rate
                            self._process_trades([{
                                'coin': coin, 'px': px_usd, 'sz': float(data['trade_volume']),
                                'side': side, 'time': int(data['trade_timestamp']), 'exchange': 'UPB'
                            }])
                            self.exchange_status['UPB']['last_msg'] = time.time()
                    except Exception: pass
        except Exception: pass

    def _check_funding_extreme(self, coin):
        d = self.data[coin]
        funding = d['funding']
        rate_pct = funding * 100
        t = CONFIG['funding_extreme_threshold']

        result = None
        if rate_pct > t:
            result = 'bearish'
            d['signals']['funding_extreme'] = {
                'active': True, 'side': 'bearish',
                'detail': f"Funding +{rate_pct:.4f}% Ã¢â‚¬â€ longs overleveraged",
            }
        elif rate_pct < -t:
            result = 'bullish'
            d['signals']['funding_extreme'] = {
                'active': True, 'side': 'bullish',
                'detail': f"Funding {rate_pct:.4f}% Ã¢â‚¬â€ shorts overleveraged",
            }
        else:
            d['signals']['funding_extreme'] = {'active': False, 'side': None, 'detail': ''}
        return result

    def _run_pressure(self):
        while self.running:
            time.sleep(CONFIG['pressure_interval'])
            with self._data_lock:
                for coin in self.coins:
                    d = self.data[coin]
                    buy_d = d['total_buy_vol'] - d['last_pressure_snap']['buys']
                    sell_d = d['total_sell_vol'] - d['last_pressure_snap']['sells']
                    d['pressure_history'].append({
                        'time': int(time.time() * 1000),
                        'buys': buy_d, 
                        'sells': sell_d, 
                        'net': buy_d - sell_d,
                        'side': d['abs_side'],
                        'detail': f"{'Sells' if d['abs_side'] == 'bullish' else 'Buys'} absorbed" if d['abs_detected'] else '',
                    })
                    d['last_pressure_snap'] = {'buys': d['total_buy_vol'], 'sells': d['total_sell_vol']}

    # ==================== STATE EXPORT ====================

    def get_state(self):
        with self._data_lock:
            state = {
                'connected': self.connected,
                'started_at': self.started_at,
                'uptime_seconds': time.time() - self.started_at if self.started_at else 0,
                'last_funding_update': self.last_funding_update,
                'last_trade_update': self.last_trade_update,
                'snapshot_loaded': self.snapshot_loaded,
                'exchange_status': self.exchange_status,
                'whale_thresholds': self.whale_thresholds,
                'log_buffer': [e for e in _mh.get_entries() if e.get('timestamp', 0) >= self.started_at],
                'coins': {}
            }
            for coin in self.coins:
                d = self.data[coin]
                state['coins'][coin] = {
                    'whale_trades': list(d['whale_trades'])[:2000],
                    'mega_whales': list(d['mega_whales']),
                    'total_buy_vol': d['total_buy_vol'],
                    'total_sell_vol': d['total_sell_vol'],
                    'buy_count': d['buy_count'],
                    'sell_count': d['sell_count'],
                    'current_buy_vol': d['current_buy_vol'],
                    'current_sell_vol': d['current_sell_vol'],
                    'current_buy_count': d['current_buy_count'],
                    'current_sell_count': d['current_sell_count'],
                    'current_since': d.get('current_since', 0),
                    'funding': d['funding'],
                    'funding_history': list(d['funding_history']),
                    'market_history': list(d['market_history']),
                    'mark_px': d['mark_px'],
                    'oracle_px': d['oracle_px'],
                    'open_interest': d['open_interest'],
                    'day_volume': d['day_volume'],
                    'last_trade_price': d['last_trade_price'],
                    'pressure_history': list(d['pressure_history']),
                    'signals': copy.deepcopy(d['signals']),
                    'alert_level': d['alert_level'],
                    'alert_label': d['alert_label'],
                    'current_bucket_buy': d['current_bucket_buy'],
                    'current_bucket_sell': d['current_bucket_sell'],
                    'abs': {
                        'snapshots_count': len(d['abs_snapshots']),
                        'detected': d['abs_detected'],
                        'side': d['abs_side'],
                        'conditions': dict(d['abs_conditions']),
                        'conditions_met': d['abs_conditions_met'],
                        'metrics': dict(d['abs_metrics']),
                        'cum_buy': d['abs_cum_buy'],
                        'cum_sell': d['abs_cum_sell'],
                        'snapshots': list(d['abs_snapshots']),
                    },
                    'volume_buckets': list(d['volume_buckets']),
                    'whale_buckets': [dict(b) for b in list(d['whale_buckets'])],
                    'regime': copy.deepcopy(d['regime']),
                }
            return state

    def _get_full_state_snapshot(self):
        return self.get_state()

    def _run_local_server(self):
        """Runs the FastAPI/uvicorn server. Routes are registered at module level."""
        port = int(os.environ.get("PORT", CONFIG['ws_port']))
        host = "0.0.0.0"
        # Block until the collector singleton is fully initialised.  Without this gate
        # there is a narrow window where uvicorn accepts the first WebSocket connection
        # before _instance is set, causing an immediate 1013 close.
        _server_ready.wait(timeout=30)
        logger.info(f"🚀 Launching Cloud-Ready server on {host}:{port}")
        uvicorn.run(app, host=host, port=port, log_level="warning")

    async def _broadcast_local(self, msg_str):
        if not self.local_clients:
            return
        
        stale = []
        for client in list(self.local_clients):
            try:
                await client.send_text(msg_str)
            except Exception:
                stale.append(client)
        
        for s in stale:
            self.local_clients.discard(s)

if __name__ == "__main__":
    collector = HyperliquidCollector.get_instance()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.shutdown()

