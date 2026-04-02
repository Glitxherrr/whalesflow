"""
WhaleFlow Persistent Data Collector
Runs as a background daemon — collects whale trades, orderflow, OI, and absorption
data continuously even when no user is viewing the dashboard.
Thread-safe singleton so Streamlit can read state at any time.
"""

import threading
import json
import time
import asyncio
import requests
import websockets
from collections import deque
from datetime import datetime


class HyperliquidCollector:
    """Singleton background collector. Starts once, runs forever."""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    cls._instance.start()
        return cls._instance

    def __init__(self):
        self.coins = ['BTC', 'ETH', 'SOL', 'PAXG', 'XRP']
        self.whale_threshold = 50000
        self.data = {}
        self._data_lock = threading.Lock()
        self.running = False
        self.connected = False
        self.started_at = None

        for coin in self.coins:
            self.data[coin] = self._new_coin_data()

    def _new_coin_data(self):
        return {
            # Whale trades
            'whale_trades': deque(maxlen=500),
            'total_buy_vol': 0,
            'total_sell_vol': 0,
            'buy_count': 0,
            'sell_count': 0,

            # Absorption accumulator (ALL trades)
            'abs_cum_buy': 0,
            'abs_cum_sell': 0,
            'abs_snapshots': deque(maxlen=12),  # 5min * 12 = 1 hour
            'abs_detected': False,
            'abs_side': None,
            'abs_conditions': {'flow': False, 'reversal': False, 'oi': False, 'funding': False},
            'abs_conditions_met': 0,
            'abs_metrics': {'cvd': 0, 'vol': 0, 'price_delta': 0, 'oi_delta': 0, 'imbalance': 50, 'funding': 0},

            # Market data (from REST API)
            'last_trade_price': 0,
            'funding': 0,
            'mark_px': 0,
            'oracle_px': 0,
            'open_interest': 0,
            'day_volume': 0,

            # 30s pressure history
            'pressure_history': deque(maxlen=30),
            'last_pressure_snap': {'buys': 0, 'sells': 0},
        }

    def start(self):
        if self.running:
            return
        self.running = True
        self.started_at = time.time()

        threading.Thread(target=self._run_ws, daemon=True, name='ws-collector').start()
        threading.Thread(target=self._run_funding, daemon=True, name='funding-poller').start()
        threading.Thread(target=self._run_absorption, daemon=True, name='abs-engine').start()
        threading.Thread(target=self._run_pressure, daemon=True, name='pressure-snap').start()

        print(f"[Collector] Started at {datetime.now().isoformat()}")

    # ---- WebSocket Trade Collector ----

    def _run_ws(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self.running:
            try:
                loop.run_until_complete(self._ws_loop())
            except Exception as e:
                print(f"[WS] Error: {e}")
                self.connected = False
                time.sleep(5)

    async def _ws_loop(self):
        uri = 'wss://api.hyperliquid.xyz/ws'
        async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
            self.connected = True
            print("[WS] Connected")

            # Subscribe to trades for ALL coins
            for coin in self.coins:
                await ws.send(json.dumps({
                    'method': 'subscribe',
                    'subscription': {'type': 'trades', 'coin': coin}
                }))

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg.get('channel') == 'trades' and 'data' in msg:
                        self._process_trades(msg['data'])
                except Exception:
                    pass

    def _process_trades(self, trades):
        with self._data_lock:
            for trade in trades:
                coin = trade.get('coin', 'BTC')
                if coin not in self.data:
                    continue

                price = float(trade['px'])
                size = float(trade['sz'])
                value = price * size
                is_buy = trade['side'] == 'B'

                d = self.data[coin]
                d['last_trade_price'] = price

                # Absorption accumulator (ALL trades)
                if is_buy:
                    d['abs_cum_buy'] += value
                else:
                    d['abs_cum_sell'] += value

                # Whale tracking
                if value >= self.whale_threshold:
                    d['whale_trades'].appendleft({
                        'time': trade.get('time', int(time.time() * 1000)),
                        'side': 'BUY' if is_buy else 'SELL',
                        'price': price,
                        'size': size,
                        'value': value,
                        'coin': coin,
                    })

                    if is_buy:
                        d['total_buy_vol'] += value
                        d['buy_count'] += 1
                    else:
                        d['total_sell_vol'] += value
                        d['sell_count'] += 1

    # ---- Funding / OI Poller ----

    def _run_funding(self):
        while self.running:
            try:
                self._fetch_funding()
            except Exception as e:
                print(f"[Funding] Error: {e}")
            time.sleep(15)

    def _fetch_funding(self):
        r = requests.post(
            'https://api.hyperliquid.xyz/info',
            json={'type': 'metaAndAssetCtxs'},
            timeout=10
        )
        data = r.json()
        universe = data[0]['universe']
        contexts = data[1]

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

    # ---- Absorption Snapshot Engine ----

    def _run_absorption(self):
        # Wait 5 min before first snapshot
        time.sleep(300)
        while self.running:
            try:
                self._take_snapshot()
                self._evaluate_absorption()
            except Exception as e:
                print(f"[Absorption] Error: {e}")
            time.sleep(300)  # Every 5 minutes

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
        print(f"[Absorption] Snapshot taken at {datetime.now().strftime('%H:%M:%S')}")

    def _evaluate_absorption(self):
        with self._data_lock:
            for coin in self.coins:
                d = self.data[coin]
                snaps = list(d['abs_snapshots'])

                if len(snaps) < 2:
                    d['abs_detected'] = False
                    d['abs_conditions_met'] = 0
                    d['abs_conditions'] = {'flow': False, 'reversal': False, 'oi': False, 'funding': False}
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

                c1 = imbalance >= 60 and total_vol > 50000
                c2 = False
                if c1:
                    c2 = (flow_is_buy and price_delta <= 0.02) or (not flow_is_buy and price_delta >= -0.02)
                c3 = oi_delta > 0.05
                c4 = (flow_is_buy and funding > 0.000005) or (not flow_is_buy and funding < -0.000005)

                d['abs_conditions'] = {'flow': c1, 'reversal': c2, 'oi': c3, 'funding': c4}
                met = sum([c1, c2, c3, c4])
                d['abs_conditions_met'] = met
                d['abs_detected'] = met == 4
                d['abs_side'] = ('bearish' if flow_is_buy else 'bullish') if d['abs_detected'] else None

    # ---- Pressure Snapshots (30s) ----

    def _run_pressure(self):
        while self.running:
            time.sleep(30)
            with self._data_lock:
                for coin in self.coins:
                    d = self.data[coin]
                    buy_delta = d['total_buy_vol'] - d['last_pressure_snap']['buys']
                    sell_delta = d['total_sell_vol'] - d['last_pressure_snap']['sells']
                    d['pressure_history'].append({
                        'time': int(time.time() * 1000),
                        'buys': buy_delta,
                        'sells': sell_delta,
                        'net': buy_delta - sell_delta,
                    })
                    d['last_pressure_snap'] = {
                        'buys': d['total_buy_vol'],
                        'sells': d['total_sell_vol'],
                    }

    # ---- State Export (thread-safe) ----

    def get_state(self):
        """Return a JSON-serializable dict of all accumulated state."""
        with self._data_lock:
            state = {
                'connected': self.connected,
                'started_at': self.started_at,
                'uptime_seconds': time.time() - self.started_at if self.started_at else 0,
                'coins': {}
            }
            for coin in self.coins:
                d = self.data[coin]
                state['coins'][coin] = {
                    'whale_trades': list(d['whale_trades']),
                    'total_buy_vol': d['total_buy_vol'],
                    'total_sell_vol': d['total_sell_vol'],
                    'buy_count': d['buy_count'],
                    'sell_count': d['sell_count'],
                    'funding': d['funding'],
                    'mark_px': d['mark_px'],
                    'oracle_px': d['oracle_px'],
                    'open_interest': d['open_interest'],
                    'day_volume': d['day_volume'],
                    'last_trade_price': d['last_trade_price'],
                    'pressure_history': list(d['pressure_history']),
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
                }
            return state
