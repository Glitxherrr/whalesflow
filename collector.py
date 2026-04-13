"""
WhaleFlow Persistent Data Collector v3
8-Signal Reversal Radar + Mega Whale tracking + Funding Flip
Runs as background daemon â€” collects continuously even when no user is viewing.
"""

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
from collections import deque
from datetime import datetime

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

# ===== LOGGING SETUP =====
os.makedirs('logs', exist_ok=True)
logger = logging.getLogger('whaleflow')
logger.setLevel(logging.INFO)
_fh = logging.handlers.RotatingFileHandler(
    'logs/collector.log', maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
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
    'whale_thresholds': {'BTC': 50000, 'ETH': 10000, 'SOL': 100, 'PAXG': 10, 'XRP': 50},
    'mega_thresholds': {'BTC': 2000000, 'ETH': 1000000, 'SOL': 500000, 'PAXG': 200000, 'XRP': 300000},
    'ws_port': 8765,
    'funding_poll_interval': 15,
    'signal_eval_interval': 15,
    'signal_initial_delay': 30,
    'snapshot_interval': 60,
    'snapshot_initial_delay': 60,
    'pressure_interval': 30,
    'signal_decay_initiative': 300,
    'signal_decay_clustering': 180,
    'clustering_window_ms': 60000,
    'clustering_min_trades': 5,
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
    'snapshot_path': 'runtime/state_snapshot.json',
    'max_trade_value': 100_000_000_000_000,
    'trade_time_tolerance_ms': 300_000,
    'dedupe_cache_size': 2000,
    'funding_history_maxlen': 6000,
    'market_history_maxlen': 6000,
    'abs_snapshots_maxlen': 320,
}


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
        return cls._instance

    def __init__(self):
        self.coins = CONFIG['coins']
        self.whale_thresholds = CONFIG['whale_thresholds']
        self.mega_thresholds = CONFIG['mega_thresholds']

        self.data = {}
        self._data_lock = threading.Lock()
        self.running = False
        self.connected = False
        self.started_at = None

        # Exchange health tracking
        self.exchanges = [
            'HL', 'BIN', 'BYB', 'OKX', 'KRK', 'CB', 
            'DRB', 'BFX', 'BGT', 'MEXC', 'UPB', 'GATE'
        ]
        self.exchange_status = {
            ex: {'connected': False, 'last_msg': 0, 'last_error': ''}
            for ex in self.exchanges
        }

        # Freshness timestamps
        self.last_funding_update = 0
        self.last_trade_update = 0
        self.snapshot_loaded = False

        # Trade deduplication
        self._dedupe_lock = threading.Lock()
        self._dedupe_set = set()
        self._dedupe_queue = deque()

        self.local_clients = set()
        self.local_loop = None

        for coin in self.coins:
            self.data[coin] = self._new_coin_data()

        # Load previous snapshot
        self._load_snapshot()

    def _new_coin_data(self):
        return {
            'whale_trades': deque(maxlen=500),
            'total_buy_vol': 0.0,
            'total_sell_vol': 0.0,
            'buy_count': 0,
            'sell_count': 0,
            'current_buy_vol': 0.0,
            'current_sell_vol': 0.0,
            'current_buy_count': 0,
            'current_sell_count': 0,
            'whale_buckets': deque(maxlen=1440),
            'mega_whales': deque(maxlen=100),
            'abs_cum_buy': 0.0,
            'abs_cum_sell': 0.0,
            'abs_snapshots': deque(maxlen=CONFIG['abs_snapshots_maxlen']),
            'abs_detected': False,
            'abs_side': None,
            'abs_conditions': {'flow': False, 'reversal': False, 'oi': False, 'funding': False},
            'abs_conditions_met': 0,
            'abs_metrics': {'cvd': 0, 'vol': 0, 'price_delta': 0, 'oi_delta': 0, 'imbalance': 50, 'funding': 0},
            'last_trade_price': 0.0,
            'funding': 0.0,
            'funding_history': deque(maxlen=CONFIG['funding_history_maxlen']),
            'market_history': deque(maxlen=CONFIG['market_history_maxlen']),
            'mark_px': 0.0,
            'oracle_px': 0.0,
            'open_interest': 0.0,
            'day_volume': 0.0,
            'pressure_history': deque(maxlen=30),
            'last_pressure_snap': {'buys': 0.0, 'sells': 0.0},
            'volume_buckets': deque(maxlen=12),
            'current_bucket_buy': 0.0,
            'current_bucket_sell': 0.0,
            'signals': {
                'absorption':   {'active': False, 'side': None, 'detail': ''},
                'cvd_divergence': {'active': False, 'side': None, 'detail': ''},
                'oi_divergence':  {'active': False, 'side': None, 'detail': ''},
                'volume_climax':  {'active': False, 'side': None, 'detail': ''},
                'funding_extreme': {'active': False, 'side': None, 'detail': ''},
                'initiative':    {'active': False, 'side': None, 'detail': '', 'time': 0},
                'clustering':    {'active': False, 'side': None, 'detail': '', 'time': 0},
            },
            'alert_level': 0,
            'alert_label': 'Quiet',
            'regime': {
                'score': 50,
                'label': 'ANALYZING\u2026',
                'css_class': '',
                'last_change_time': 0,
                'price_history': [],
                'range_score': 0,
                'volume_score': 0,
                'cvd_score': 0,
                'balance_score': 0,
            },
        }

    # ==================== LIFECYCLE ====================

    def start(self):
        if self.running:
            return
        self.running = True
        if self.started_at is None:
            self.started_at = time.time()

        # Graceful shutdown handlers (signal only works from main thread)
        try:
            _signal.signal(_signal.SIGINT, lambda s, f: self.shutdown())
            _signal.signal(_signal.SIGTERM, lambda s, f: self.shutdown())
        except (ValueError, OSError, AttributeError):
            pass  # Running in Streamlit worker thread â€” atexit still works
        atexit.register(self.shutdown)

        threading.Thread(target=self._run_ws, daemon=True, name='ws').start()
        threading.Thread(target=self._run_ws_binance_spot, daemon=True, name='bin_spot').start()
        threading.Thread(target=self._run_ws_binance_futures, daemon=True, name='bin_fut').start()
        threading.Thread(target=self._run_ws_bybit_linear, daemon=True, name='byb_perp').start()
        threading.Thread(target=self._run_ws_bybit_spot, daemon=True, name='byb_spot').start()
        threading.Thread(target=self._run_ws_okx, daemon=True, name='okx').start()
        threading.Thread(target=self._run_ws_kraken_spot, daemon=True, name='krk_spot').start()
        threading.Thread(target=self._run_ws_kraken_futures, daemon=True, name='krk_fut').start()
        threading.Thread(target=self._run_ws_coinbase, daemon=True, name='cb').start()
        
        # New aggressive expansion exchanges
        threading.Thread(target=self._run_ws_deribit, daemon=True, name='drb').start()
        threading.Thread(target=self._run_ws_bitfinex, daemon=True, name='bfx').start()
        threading.Thread(target=self._run_ws_bitget, daemon=True, name='bgt').start()
        threading.Thread(target=self._run_ws_bitget_futures, daemon=True, name='bgt_fut').start()
        threading.Thread(target=self._run_ws_mexc, daemon=True, name='mexc').start()
        threading.Thread(target=self._run_ws_mexc_futures, daemon=True, name='mexc_fut').start()
        threading.Thread(target=self._run_ws_upbit, daemon=True, name='upb').start()
        threading.Thread(target=self._run_ws_gate, daemon=True, name='gate').start()
        threading.Thread(target=self._run_ws_gate_futures, daemon=True, name='gate_fut').start()

        threading.Thread(target=self._run_funding, daemon=True, name='funding').start()
        threading.Thread(target=self._run_snapshots, daemon=True, name='snapshots').start()
        threading.Thread(target=self._run_signals, daemon=True, name='signals').start()
        threading.Thread(target=self._run_pressure, daemon=True, name='pressure').start()
        threading.Thread(target=self._run_local_server, daemon=True, name='local_ws').start()

        # Wire live log broadcasting through the local WS
        def _broadcast_log(entry):
            if self.local_loop and self.local_clients:
                msg = json.dumps({'channel': 'log', 'data': entry})
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_local(msg), self.local_loop
                )
        _mh.set_broadcaster(_broadcast_log)

        logger.info(f"Collector started at {datetime.now().isoformat()}")

    def shutdown(self):
        if not self.running:
            return
        logger.info("Shutting down collector...")
        self.running = False
        self._save_snapshot()
        logger.info("Collector shutdown complete.")

    # ==================== SNAPSHOT PERSISTENCE ====================

    def _save_snapshot(self):
        try:
            state = self.get_state()
            snap_dir = os.path.dirname(CONFIG['snapshot_path'])
            if snap_dir:
                os.makedirs(snap_dir, exist_ok=True)
            tmp_path = CONFIG['snapshot_path'] + '.tmp'
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(state, f)
            os.replace(tmp_path, CONFIG['snapshot_path'])
            logger.info(f"Snapshot saved to {CONFIG['snapshot_path']}")
        except Exception:
            logger.exception("Failed to save snapshot")

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
            if 'exchange_status' in state:
                self.exchange_status.update(state['exchange_status'])
            
            # Restore log buffer
            if state.get('log_buffer'):
                _mh.buffer.clear()
                _mh.buffer.extend(state['log_buffer'])

            with self._data_lock:
                for coin in self.coins:
                    sc = state['coins'].get(coin)
                    if not sc:
                        continue
                    d = self.data[coin]

                    for key in ['total_buy_vol', 'total_sell_vol', 'buy_count', 'sell_count',
                                'current_buy_vol', 'current_sell_vol', 'current_buy_count', 'current_sell_count',
                                'funding', 'mark_px', 'oracle_px', 'open_interest', 'day_volume', 'last_trade_price']:
                        if key in sc:
                            d[key] = sc[key]

                    if sc.get('whale_trades'):
                        d['whale_trades'] = deque(sc['whale_trades'][:500], maxlen=500)
                    if sc.get('mega_whales'):
                        d['mega_whales'] = deque(sc['mega_whales'][:100], maxlen=100)
                    if sc.get('whale_buckets'):
                        d['whale_buckets'] = deque(sc['whale_buckets'][-1440:], maxlen=1440)
                    if sc.get('pressure_history'):
                        d['pressure_history'] = deque(sc['pressure_history'][-30:], maxlen=30)
                    if sc.get('volume_buckets'):
                        d['volume_buckets'] = deque(sc['volume_buckets'][-12:], maxlen=12)
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
            logger.info("WS HL connected")

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

    # ==================== WEBSOCKET - Binance ====================

    def _run_ws_binance_spot(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_binance_spot())
            except Exception as e:
                err = str(e)
                if "451" in err:
                    logger.warning("Binance Spot geo-blocked on this host.")
                    break
                logger.error(f"WS Binance Spot error: {e}")
                time.sleep(5)

    async def _ws_loop_binance_spot(self):
        streams = "/".join([f"{coin.lower()}usdt@aggTrade" for coin in self.coins])
        url = f"wss://stream.binance.us:9443/stream?streams={streams}"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BIN']['connected'] = True
            logger.info("WS Binance.US Spot connected")
            async for raw in ws:
                try:
                    data = json.loads(raw)['data']
                    self._process_trades([{
                        'coin': data['s'].replace('USDT', ''),
                        'px': float(data['p']), 'sz': float(data['q']),
                        'side': 'B' if not data['m'] else 'S',
                        'time': int(data['E']), 'exchange': 'BIN_SPOT'
                    }])
                    self.exchange_status['BIN']['last_msg'] = time.time()
                except Exception: pass

    def _run_ws_binance_futures(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_binance_futures())
            except Exception as e:
                err = str(e)
                if "451" in err:
                    logger.warning("Binance Futures geo-blocked on this host.")
                    break
                logger.error(f"WS Binance Futures error: {e}")
                time.sleep(5)

    async def _ws_loop_binance_futures(self):
        streams = "/".join([f"{coin.lower()}usdt@aggTrade" for coin in self.coins])
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BIN']['connected'] = True
            logger.info("WS Binance Futures connected")
            async for raw in ws:
                try:
                    data = json.loads(raw)['data']
                    self._process_trades([{
                        'coin': data['s'].replace('USDT', ''),
                        'px': float(data['p']), 'sz': float(data['q']),
                        'side': 'B' if not data['m'] else 'S',
                        'time': int(data['E']), 'exchange': 'BIN_FUT'
                    }])
                    self.exchange_status['BIN']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - Bybit ====================

    def _run_ws_bybit_linear(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_bybit_linear())
            except Exception as e:
                logger.error(f"WS Bybit Linear error: {e}")
                self.exchange_status['BYB']['connected'] = False
                time.sleep(5)

    async def _ws_loop_bybit_linear(self):
        url = "wss://stream.bybit.com/v5/public/linear"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BYB']['connected'] = True
            logger.info("WS Bybit Linear connected")
            args = [f"publicTrade.{coin}USDT" for coin in self.coins]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if 'data' in msg and isinstance(msg['data'], list):
                        parsed_trades = []
                        for t in msg['data']:
                            coin = msg['topic'].split('.')[1].replace('USDT', '')
                            if coin in self.coins:
                                parsed_trades.append({
                                    'coin': coin, 'px': float(t['p']), 'sz': float(t['v']),
                                    'side': 'B' if t['S'] == 'Buy' else 'S',
                                    'time': int(t['T']), 'exchange': 'BYB'
                                })
                        if parsed_trades:
                            self._process_trades(parsed_trades)
                            self.exchange_status['BYB']['last_msg'] = time.time()
                except Exception:
                    logger.warning("WS Bybit parse error", exc_info=True)

    def _run_ws_bybit_spot(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_bybit_spot())
            except Exception as e:
                logger.error(f"WS Bybit Spot error: {e}")
                self.exchange_status['BYB']['connected'] = False
                time.sleep(5)

    async def _ws_loop_bybit_spot(self):
        url = "wss://stream.bybit.com/v5/public/spot"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            logger.info("WS Bybit Spot connected")
            args = [f"publicTrade.{coin}USDT" for coin in self.coins]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if 'data' in msg and isinstance(msg['data'], list):
                        for t in msg['data']:
                            coin = msg['topic'].split('.')[1].replace('USDT', '')
                            self._process_trades([{
                                'coin': coin, 'px': float(t['p']), 'sz': float(t['v']),
                                'side': 'B' if t['S'] == 'Buy' else 'S',
                                'time': int(t['T']), 'exchange': 'BYB_SPOT'
                            }])
                            self.exchange_status['BYB']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - OKX ====================

    def _run_ws_okx(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_okx())
            except Exception as e:
                logger.error(f"WS OKX connection error: {e}")
                self.exchange_status['OKX']['connected'] = False
                self.exchange_status['OKX']['last_error'] = str(e)
                time.sleep(5)

    async def _ws_loop_okx(self):
        url = "wss://ws.okx.com:8443/ws/v5/public"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['OKX']['connected'] = True
            logger.info("WS OKX connected")
            # Subscribe to both SWAP (Perps) and Spot
            args = []
            for coin in self.coins:
                args.append({"channel": "trades", "instId": f"{coin}-USDT-SWAP"})
                args.append({"channel": "trades", "instId": f"{coin}-USDT"})
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('arg', {}).get('channel') == 'trades' and 'data' in msg:
                        parsed_trades = []
                        for t in msg['data']:
                            coin = t['instId'].split('-')[0]
                            if coin in self.coins:
                                parsed_trades.append({
                                    'coin': coin, 'px': float(t['px']), 'sz': float(t['sz']),
                                    'side': 'B' if t['side'] == 'buy' else 'S',
                                    'time': int(t['ts']), 'exchange': 'OKX'
                                })
                        if parsed_trades:
                            self._process_trades(parsed_trades)
                            self.exchange_status['OKX']['last_msg'] = time.time()
                except Exception:
                    logger.warning("WS OKX parse error", exc_info=True)

    # ==================== WEBSOCKET - Kraken ====================

    def _run_ws_kraken_spot(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_kraken_spot())
            except Exception as e:
                logger.error(f"WS Kraken Spot error: {e}")
                self.exchange_status['KRK']['connected'] = False
                time.sleep(5)

    async def _ws_loop_kraken_spot(self):
        url = "wss://ws.kraken.com/v2"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['KRK']['connected'] = True
            logger.info("WS Kraken Spot connected")
            args = [f"{coin}/USD" for coin in self.coins]
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": args}
            }))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('channel') == 'trade' and msg.get('type') == 'update':
                        parsed_trades = []
                        for t in msg.get('data', []):
                            coin = t['symbol'].split('/')[0]
                            if coin in self.coins:
                                dtime = datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00'))
                                parsed_trades.append({
                                    'coin': coin, 'px': float(t['price']), 'sz': float(t['qty']),
                                    'side': 'B' if t['side'] == 'buy' else 'S',
                                    'time': int(dtime.timestamp() * 1000), 'exchange': 'KRK'
                                })
                        if parsed_trades:
                            self._process_trades(parsed_trades)
                            self.exchange_status['KRK']['last_msg'] = time.time()
                except Exception:
                    logger.warning("WS Kraken parse error", exc_info=True)

    def _run_ws_kraken_futures(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_kraken_futures())
            except Exception as e:
                logger.error(f"WS Kraken Futures error: {e}")
                self.exchange_status['KRK']['connected'] = False
                time.sleep(5)

    async def _ws_loop_kraken_futures(self):
        url = "wss://futures.kraken.com/ws/v1"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            logger.info("WS Kraken Futures connected")
            # Kraken Futures symbols are like pi_btcusd
            args = [f"pi_{coin.lower()}usd" for coin in self.coins]
            await ws.send(json.dumps({
                "event": "subscribe",
                "feed": "trade",
                "product_ids": args
            }))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('feed') == 'trade' and 'qty' in msg:
                        coin = msg['product_id'].split('_')[1].replace('usd', '').upper()
                        self._process_trades([{
                            'coin': coin, 'px': float(msg['price']), 'sz': float(msg['qty']),
                            'side': 'B' if msg['side'] == 'buy' else 'S',
                            'time': int(msg['time']), 'exchange': 'KRK_FUT'
                        }])
                        self.exchange_status['KRK']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - Coinbase ====================

    def _run_ws_coinbase(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_coinbase())
            except Exception as e:
                logger.error(f"WS Coinbase connection error: {e}")
                self.exchange_status['CB']['connected'] = False
                self.exchange_status['CB']['last_error'] = str(e)
                time.sleep(5)

    async def _ws_loop_coinbase(self):
        url = "wss://advanced-trade-ws.coinbase.com"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['CB']['connected'] = True
            logger.info("WS Coinbase (Spot + Perp) connected")
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

    # ==================== WEBSOCKET - Deribit ====================

    def _run_ws_deribit(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_deribit())
            except Exception as e:
                logger.error(f"WS Deribit error: {e}")
                self.exchange_status['DRB']['connected'] = False
                time.sleep(10)

    async def _ws_loop_deribit(self):
        url = "wss://www.deribit.com/ws/api/v2"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['DRB']['connected'] = True
            logger.info("WS Deribit (All Futures) connected")
            # Subscribe to all trades for our coins (Perpetuals, Dated, and Spot)
            for coin in self.coins:
               if coin == 'PAXG': continue
               await ws.send(json.dumps({
                   "jsonrpc": "2.0", "id": 1, "method": "public/subscribe",
                   "params": {"channels": [f"trades.{coin}.raw", f"trades.{coin}_USDC.raw", f"trades.{coin}_USDT.raw"]}
               }))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if 'params' in msg and 'data' in msg['params']:
                        for t in msg['params']['data']:
                            inst = t['instrument_name']
                            if 'OPTION' in inst: continue # Skip options for now
                            coin = inst.split('-')[0]
                            self._process_trades([{
                                'coin': coin, 'px': float(t['price']), 'sz': float(t['amount']),
                                'side': 'B' if t['direction'] == 'buy' else 'S',
                                'time': int(t['timestamp']), 'exchange': 'DRB_FUT'
                            }])
                            self.exchange_status['DRB']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - Bitfinex ====================

    def _run_ws_bitfinex(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_bitfinex())
            except Exception as e:
                if "451" in str(e):
                    logger.warning("Bitfinex geo-blocked.")
                    break
                logger.error(f"WS Bitfinex error: {e}")
                self.exchange_status['BFX']['connected'] = False
                time.sleep(10)

    async def _ws_loop_bitfinex(self):
        url = "wss://api-pub.bitfinex.com/ws/2"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BFX']['connected'] = True
            logger.info("WS Bitfinex connected")
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
                if "close frame" in err_str or "rejected" in err_str or "403" in err_str:
                    logger.info("Bitget Spot: Regional Block detected (Streamlit Cloud US).")
                    self.exchange_status['BGT']['last_error'] = "Geo-Blocked"
                    break 
                logger.error(f"WS Bitget error: {e}")
                time.sleep(10)

    async def _ws_loop_bitget(self):
        # Using alternate domain to try and bypass regional blocks
        url = "wss://ws.bitgetapi.com/v2/ws/public"
        headers = {"Origin": "https://www.bitget.com"}
        async with websockets.connect(url, extra_headers=headers, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BGT']['connected'] = True
            args = [{"instType": "SPOT", "channel": "trade", "instId": f"{coin}USDT"} for coin in self.coins]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if 'data' in msg:
                        for t in msg['data']:
                            coin = t['instId'].replace('USDT', '')
                            self._process_trades([{
                                'coin': coin, 'px': float(t['price']), 'sz': float(t['size']),
                                'side': 'B' if t['side'] == 'buy' else 'S',
                                'time': int(t['ts']), 'exchange': 'BGT_SPOT'
                            }])
                            self.exchange_status['BGT']['last_msg'] = time.time()
                except Exception: pass

    def _run_ws_bitget_futures(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_bitget_futures())
            except Exception as e:
                err_str = str(e).lower()
                self.exchange_status['BGT']['connected'] = False
                if "close frame" in err_str or "rejected" in err_str or "403" in err_str:
                    logger.info("Bitget Futures: Regional Block detected.")
                    break
                logger.error(f"WS Bitget Futures error: {e}")
                time.sleep(10)

    async def _ws_loop_bitget_futures(self):
        # Using alternate domain to try and bypass regional blocks
        url = "wss://ws.bitgetapi.com/v2/ws/public"
        headers = {"Origin": "https://www.bitget.com"}
        async with websockets.connect(url, extra_headers=headers, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['BGT']['connected'] = True
            args = [{"instType": "usdt-futures", "channel": "trade", "instId": f"{coin}USDT"} for coin in self.coins]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if 'data' in msg:
                        for t in msg['data']:
                            coin = t['instId'].replace('USDT', '')
                            self._process_trades([{
                                'coin': coin, 'px': float(t['price']), 'sz': float(t['size']),
                                'side': 'B' if t['side'] == 'buy' else 'S',
                                'time': int(t['ts']), 'exchange': 'BGT_FUT'
                            }])
                            self.exchange_status['BGT']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - MEXC ====================

    def _run_ws_mexc(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_mexc())
            except Exception as e:
                logger.error(f"WS MEXC error: {e}")
                self.exchange_status['MEXC']['connected'] = False
                time.sleep(10)

    async def _ws_loop_mexc(self):
        url = "wss://wbs.mexc.com/ws"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['MEXC']['connected'] = True
            subs = [f"spot@public.deals.v3.api@{coin}USDT" for coin in self.coins]
            await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": subs}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if 'd' in msg and 'deals' in msg['d']:
                        coin = msg['s'].replace('USDT', '')
                        for t in msg['d']['deals']:
                            self._process_trades([{
                                'coin': coin, 'px': float(t['p']), 'sz': float(t['v']),
                                'side': 'B' if t['S'] == 1 else 'S',
                                'time': int(t['t']), 'exchange': 'MEXC'
                            }])
                            self.exchange_status['MEXC']['last_msg'] = time.time()
                except Exception: pass

    def _run_ws_mexc_futures(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_mexc_futures())
            except Exception as e:
                logger.error(f"WS MEXC Futures error: {e}")
                self.exchange_status['MEXC']['connected'] = False
                time.sleep(10)

    async def _ws_loop_mexc_futures(self):
        url = "wss://contract.mexc.com/edge"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['MEXC']['connected'] = True
            for coin in self.coins:
                await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": f"{coin}_USDT"}}))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('channel') == 'push.deal':
                        d = msg['data']
                        coin = msg['symbol'].split('_')[0]
                        self._process_trades([{
                            'coin': coin, 'px': float(d['p']), 'sz': float(d['v']),
                            'side': 'B' if d['T'] == 1 else 'S',
                            'time': int(d['t']), 'exchange': 'MEXC_FUT'
                        }])
                        self.exchange_status['MEXC']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - Upbit ====================

    def _run_ws_upbit(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_upbit())
            except Exception as e:
                logger.error(f"WS Upbit error: {e}")
                self.exchange_status['UPB']['connected'] = False
                time.sleep(10)

    async def _ws_loop_upbit(self):
        url = "wss://api.upbit.com/websocket/v1"
        krw_usd = 1380.0 # Approximate conversion for volume normalization
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['UPB']['connected'] = True
            codes = [f"KRW-{coin}" for coin in self.coins if coin != 'PAXG']
            await ws.send(json.dumps([{"ticket": "whaleflow"}, {"type": "trade", "codes": codes}]))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    coin = msg['code'].split('-')[1]
                    # Normalize KRW to USD
                    px_usd = float(msg['trade_price']) / krw_usd
                    sz_units = float(msg['trade_volume'])
                    self._process_trades([{
                        'coin': coin, 'px': px_usd, 'sz': sz_units,
                        'side': 'B' if msg['ask_bid'] == 'BID' else 'S',
                        'time': int(msg['trade_timestamp']), 'exchange': 'UPB'
                    }])
                    self.exchange_status['UPB']['last_msg'] = time.time()
                except Exception: pass

    # ==================== WEBSOCKET - Gate.io ====================

    def _run_ws_gate(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_gate())
            except Exception as e:
                logger.error(f"WS Gate error: {e}")
                self.exchange_status['GATE']['connected'] = False
                time.sleep(10)

    async def _ws_loop_gate(self):
        url = "wss://api.gateio.ws/ws/v4/"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self.exchange_status['GATE']['connected'] = True
            args = [f"{coin}_USDT" for coin in self.coins]
            await ws.send(json.dumps({
                "time": int(time.time()), "channel": "spot.trades",
                "event": "subscribe", "payload": args
            }))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('event') == 'update' and 'result' in msg:
                        t = msg['result']
                        coin = t['currency_pair'].split('_')[0]
                        self._process_trades([{
                            'coin': coin, 'px': float(t['price']), 'sz': float(t['amount']),
                            'side': 'B' if t['side'] == 'buy' else 'S',
                            'time': int(float(t['create_time']) * 1000), 'exchange': 'GATE'
                        }])
                        self.exchange_status['GATE']['last_msg'] = time.time()
                except Exception: pass

    def _run_ws_gate_futures(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop_gate_futures())
            except Exception as e:
                logger.error(f"WS Gate Futures error: {e}")
                self.exchange_status['GATE']['connected'] = False
                time.sleep(10)

    async def _ws_loop_gate_futures(self):
        url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            args = [f"{coin}_USDT" for coin in self.coins]
            await ws.send(json.dumps({
                "time": int(time.time()), "channel": "futures.trades",
                "event": "subscribe", "payload": args
            }))
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('event') == 'update' and 'result' in msg:
                        for t in msg['result']:
                            coin = t['contract'].split('_')[0]
                            self._process_trades([{
                                'coin': coin, 'px': float(t['p']), 'sz': float(t['size']),
                                'side': 'B' if float(t['size']) > 0 else 'S',
                                'time': int(float(t['create_time']) * 1000), 'exchange': 'GATE_FUT'
                            }])
                            self.exchange_status['GATE']['last_msg'] = time.time()
                except Exception: pass

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
                coin_threshold = self.whale_thresholds.get(coin, 50000)
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
        except Exception as e:
            logger.warning(f"Funding API request failed: {e}")
            return
            
        if not data or not isinstance(data, list) or len(data) < 2:
            logger.warning(f"Unexpected funding data format received: {data}")
            return
            
        if not data[0] or not isinstance(data[0], dict) or 'universe' not in data[0]:
            logger.warning("Missing 'universe' key in funding data.")
            return

        universe = data[0]['universe']
        contexts = data[1] if isinstance(data[1], list) else []

        with self._data_lock:
            for coin in self.coins:
                idx = next((i for i, u in enumerate(universe) if u['name'] == coin), -1)
                if idx == -1 or not contexts[idx]:
                    continue
                ctx = contexts[idx]
                d = self.data[coin]
                d['funding'] = float(ctx.get('funding', '0'))
                d['mark_px'] = float(ctx.get('markPx', '0'))
                d['oracle_px'] = float(ctx.get('oraclePx', '0'))
                d['open_interest'] = float(ctx.get('openInterest', '0'))
                d['day_volume'] = float(ctx.get('dayNtlVlm', '0'))
                d['funding_history'].append({'time': time.time(), 'funding': d['funding']})
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

    # ==================== SIGNAL EVALUATOR (15s) ====================

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
                    'detail': f"Buy volume {ratio:.1f}x avg â€” possible blow-off top",
                }
            elif buy_pct < CONFIG['volume_climax_buy_low']:
                result = 'bullish'
                d['signals']['volume_climax'] = {
                    'active': True, 'side': 'bullish',
                    'detail': f"Sell volume {ratio:.1f}x avg â€” possible capitulation bottom",
                }
            else:
                d['signals']['volume_climax'] = {'active': False, 'side': None, 'detail': ''}
        else:
            d['signals']['volume_climax'] = {'active': False, 'side': None, 'detail': ''}
        return result

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
                'detail': f"Funding +{rate_pct:.4f}% â€” longs overleveraged",
            }
        elif rate_pct < -t:
            result = 'bullish'
            d['signals']['funding_extreme'] = {
                'active': True, 'side': 'bullish',
                'detail': f"Funding {rate_pct:.4f}% â€” shorts overleveraged",
            }
        else:
            d['signals']['funding_extreme'] = {'active': False, 'side': None, 'detail': ''}
        return result

    # ==================== PRESSURE SNAPSHOTS ====================

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
                        'buys': buy_d, 'sells': sell_d, 'net': buy_d - sell_d,
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
                'log_buffer': _mh.get_entries(),
                'coins': {}
            }
            for coin in self.coins:
                d = self.data[coin]
                state['coins'][coin] = {
                    'whale_trades': list(d['whale_trades']),
                    'mega_whales': list(d['mega_whales']),
                    'total_buy_vol': d['total_buy_vol'],
                    'total_sell_vol': d['total_sell_vol'],
                    'buy_count': d['buy_count'],
                    'sell_count': d['sell_count'],
                    'current_buy_vol': d['current_buy_vol'],
                    'current_sell_vol': d['current_sell_vol'],
                    'current_buy_count': d['current_buy_count'],
                    'current_sell_count': d['current_sell_count'],
                    'funding': d['funding'],
                    'funding_history': list(d['funding_history']),
                    'market_history': list(d['market_history']),
                    'mark_px': d['mark_px'],
                    'oracle_px': d['oracle_px'],
                    'open_interest': d['open_interest'],
                    'day_volume': d['day_volume'],
                    'last_trade_price': d['last_trade_price'],
                    'pressure_history': list(d['pressure_history']),
                    'signals': d['signals'],
                    'alert_level': d['alert_level'],
                    'alert_label': d['alert_label'],
                    'current_bucket_buy': d['current_bucket_buy'],
                    'current_bucket_sell': d['current_bucket_sell'],
                    'abs': {
                        'snapshots_count': len(d['abs_snapshots']),
                        'detected': d['abs_detected'],
                        'side': d['abs_side'],
                        'conditions': d['abs_conditions'],
                        'conditions_met': d['abs_conditions_met'],
                        'metrics': d['abs_metrics'],
                        'cum_buy': d['abs_cum_buy'],
                        'cum_sell': d['abs_cum_sell'],
                        'snapshots': list(d['abs_snapshots']),
                    },
                    'volume_buckets': list(d['volume_buckets']),
                    'whale_buckets': [dict(b) for b in d['whale_buckets']],
                    'regime': d['regime'],
                }
            return state

    # ==================== LOCAL WS SERVER ====================

    def _run_local_server(self):
        self.local_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.local_loop)

        async def main():
            async with websockets.serve(self._local_ws_handler, "127.0.0.1", CONFIG['ws_port']):
                logger.info(f"Local WS Server running on ws://127.0.0.1:{CONFIG['ws_port']}")
                while self.running:
                    await asyncio.sleep(1)

        try:
            self.local_loop.run_until_complete(main())
        except Exception as e:
            logger.error(f"Local WS Server error: {e}")

    async def _local_ws_handler(self, websocket):
        self.local_clients.add(websocket)
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get('method') == 'ping':
                        await websocket.send(json.dumps({'channel': 'pong'}))
                    elif data.get('method') == 'clear_current':
                        clear_time = int(time.time() * 1000)
                        with self._data_lock:
                            for c in self.coins:
                                self.data[c]['current_buy_vol'] = 0.0
                                self.data[c]['current_sell_vol'] = 0.0
                                self.data[c]['current_buy_count'] = 0
                                self.data[c]['current_sell_count'] = 0
                        # Synchronize all connected devices
                        await self._broadcast_local(json.dumps({
                            'channel': 'all_clients_clear',
                            'clear_time': clear_time
                        }))
                except Exception:
                    pass
        except Exception:
            pass  # Client disconnected
        finally:
            self.local_clients.discard(websocket)

    async def _broadcast_local(self, msg_str):
        clients = list(self.local_clients)
        if not clients:
            return
        stale = []
        for client in clients:
            try:
                await client.send(msg_str)
            except Exception:
                stale.append(client)
        for client in stale:
            self.local_clients.discard(client)






