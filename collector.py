"""
WhaleFlow Persistent Data Collector v2
8-Signal Reversal Radar + Mega Whale tracking + Funding Flip
Runs as background daemon — collects continuously even when no user is viewing.
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
    """Singleton background collector."""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    inst = cls()
                    cls._instance = inst
                    inst.start()
        return cls._instance

    def __init__(self):
        self.coins = ['BTC', 'ETH', 'SOL', 'PAXG', 'XRP']
        self.whale_threshold = 50000

        # Coin-specific mega thresholds for Aggressive Initiative
        self.mega_thresholds = {
            'BTC': 2000000, 'ETH': 1000000, 'SOL': 500000,
            'PAXG': 200000, 'XRP': 300000,
        }

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
            'total_buy_vol': 0.0,
            'total_sell_vol': 0.0,
            'buy_count': 0,
            'sell_count': 0,

            # Mega whales (initiative + clustering events)
            'mega_whales': deque(maxlen=100),

            # Absorption accumulator
            'abs_cum_buy': 0.0,
            'abs_cum_sell': 0.0,
            'abs_snapshots': deque(maxlen=12),
            'abs_detected': False,
            'abs_side': None,
            'abs_conditions': {'flow': False, 'reversal': False, 'oi': False, 'funding': False},
            'abs_conditions_met': 0,
            'abs_metrics': {'cvd': 0, 'vol': 0, 'price_delta': 0, 'oi_delta': 0, 'imbalance': 50, 'funding': 0},

            # Market data
            'last_trade_price': 0.0,
            'funding': 0.0,
            'mark_px': 0.0,
            'oracle_px': 0.0,
            'open_interest': 0.0,
            'day_volume': 0.0,

            # Pressure history
            'pressure_history': deque(maxlen=30),
            'last_pressure_snap': {'buys': 0.0, 'sells': 0.0},

            # Volume buckets (5-min) for climax detection
            'volume_buckets': deque(maxlen=12),
            'current_bucket_buy': 0.0,
            'current_bucket_sell': 0.0,

            # 8-Signal Reversal Radar
            'signals': {
                'absorption':   {'active': False, 'side': None, 'detail': ''},
                'cvd_divergence': {'active': False, 'side': None, 'detail': ''},
                'oi_divergence':  {'active': False, 'side': None, 'detail': ''},
                'volume_climax':  {'active': False, 'side': None, 'detail': ''},
                'funding_extreme': {'active': False, 'side': None, 'detail': ''},
                'funding_flip':   {'active': False, 'side': None, 'detail': '', 'time': 0, 'acknowledged': False},
                'initiative':    {'active': False, 'side': None, 'detail': '', 'time': 0},
                'clustering':    {'active': False, 'side': None, 'detail': '', 'time': 0},
            },
            'alert_level': 0,
            'alert_label': 'Quiet',

            # Funding Flip tracking
            'flip_state': {
                'last_sign': None,
                'last_sign_time': 0,
                'last_funding_value': 0,
                'flip_history': [],
                'chaos_active': False,
                'chaos_resolved': False,
                'chaos_resolved_side': None,
                'chaos_resolved_time': 0,
            },

            # Market Regime detector (persistent)
            'regime': {
                'score': 50,
                'label': 'ANALYZING\u2026',
                'css_class': '',
                'last_change_time': 0,
                'price_history': [],     # [{time, price}] last 60 min
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
        self.started_at = time.time()

        threading.Thread(target=self._run_ws, daemon=True, name='ws').start()
        threading.Thread(target=self._run_funding, daemon=True, name='funding').start()
        threading.Thread(target=self._run_snapshots, daemon=True, name='snapshots').start()
        threading.Thread(target=self._run_signals, daemon=True, name='signals').start()
        threading.Thread(target=self._run_pressure, daemon=True, name='pressure').start()

        print(f"[Collector] Started at {datetime.now().isoformat()}")

    # ==================== WEBSOCKET ====================

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
        async with websockets.connect('wss://api.hyperliquid.xyz/ws', ping_interval=20, ping_timeout=10) as ws:
            self.connected = True
            print("[WS] Connected")

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

    # ==================== TRADE PROCESSING ====================

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
                trade_time = trade.get('time', int(time.time() * 1000))

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
                if value >= self.whale_threshold:
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
                        # Add to mega whales
                        d['mega_whales'].appendleft({
                            'time': trade_time, 'side': 'BUY' if is_buy else 'SELL',
                            'price': price, 'size': size, 'value': value,
                            'coin': coin, 'mega_type': 'initiative',
                        })

                    whale_entry = {
                        'time': trade_time, 'side': 'BUY' if is_buy else 'SELL',
                        'price': price, 'size': size, 'value': value, 'coin': coin,
                        'is_mega': is_mega, 'mega_type': mega_type,
                    }
                    d['whale_trades'].appendleft(whale_entry)

                    if is_buy:
                        d['total_buy_vol'] += value
                        d['buy_count'] += 1
                    else:
                        d['total_sell_vol'] += value
                        d['sell_count'] += 1

                    # Check Whale Clustering
                    self._check_clustering(coin)

    def _check_clustering(self, coin):
        """5+ same-side whale trades within 60 seconds = clustering."""
        d = self.data[coin]
        now_ms = time.time() * 1000
        cutoff = now_ms - 60000

        recent = [t for t in list(d['whale_trades'])[:30] if t['time'] >= cutoff]
        if len(recent) < 5:
            return

        buys = [t for t in recent if t['side'] == 'BUY']
        sells = [t for t in recent if t['side'] == 'SELL']

        cluster_side = None
        cluster_trades = []
        if len(buys) >= 5:
            cluster_side = 'bullish'
            cluster_trades = buys
        elif len(sells) >= 5:
            cluster_side = 'bearish'
            cluster_trades = sells

        if cluster_side:
            total_val = sum(t['value'] for t in cluster_trades)
            count = len(cluster_trades)
            d['signals']['clustering'] = {
                'active': True, 'side': cluster_side,
                'detail': f"{count} {'BUY' if cluster_side == 'bullish' else 'SELL'} trades (${total_val/1e6:.2f}M) in 60s",
                'time': time.time(),
            }
            d['mega_whales'].appendleft({
                'time': int(time.time() * 1000),
                'side': 'BUY' if cluster_side == 'bullish' else 'SELL',
                'price': cluster_trades[0]['price'],
                'size': sum(t['size'] for t in cluster_trades),
                'value': total_val, 'coin': coin,
                'mega_type': 'clustering', 'cluster_count': count,
            })

    # ==================== FUNDING / OI POLLER ====================

    def _run_funding(self):
        while self.running:
            try:
                self._fetch_funding()
            except Exception as e:
                print(f"[Funding] Error: {e}")
            time.sleep(15)

    def _fetch_funding(self):
        r = requests.post('https://api.hyperliquid.xyz/info',
                          json={'type': 'metaAndAssetCtxs'}, timeout=10)
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

                # --- Funding Flip detection ---
                fs = d['flip_state']
                funding = d['funding']
                rate_pct = funding * 100
                current_sign = 'positive' if funding >= 0 else 'negative'
                now = time.time()

                if fs['last_sign'] is None:
                    fs['last_sign'] = current_sign
                    fs['last_sign_time'] = now
                    fs['last_funding_value'] = rate_pct
                    continue

                if current_sign != fs['last_sign'] and abs(fs['last_funding_value']) >= 0.005:
                    # Flip detected
                    fs['flip_history'].append({
                        'time': now,
                        'from': fs['last_sign'],
                        'to': current_sign,
                        'from_rate': fs['last_funding_value'],
                        'to_rate': rate_pct,
                    })
                    if len(fs['flip_history']) > 20:
                        fs['flip_history'] = fs['flip_history'][-20:]

                    fs['chaos_resolved'] = False
                    fs['chaos_resolved_side'] = None
                    fs['chaos_resolved_time'] = 0

                    side = 'bullish' if current_sign == 'negative' else 'bearish'
                    d['signals']['funding_flip'] = {
                        'active': True, 'side': side,
                        'detail': f"Flipped {fs['last_sign']} -> {current_sign} ({fs['last_funding_value']:.4f}% -> {rate_pct:.4f}%)",
                        'time': now, 'acknowledged': False,
                    }

                    fs['last_sign'] = current_sign
                    fs['last_sign_time'] = now
                elif current_sign == fs['last_sign']:
                    # Check chaos resolution: same sign for 2 hours
                    if fs['flip_history'] and not fs['chaos_resolved'] and now - fs['last_sign_time'] >= 7200:
                        fs['chaos_resolved'] = True
                        fs['chaos_resolved_side'] = 'bullish' if current_sign == 'positive' else 'bearish'
                        fs['chaos_resolved_time'] = now
                        fs['chaos_active'] = False

                # Chaos stays active as long as there are flips and no resolution
                fs['chaos_active'] = len(fs['flip_history']) > 0 and not fs['chaos_resolved']
                fs['last_funding_value'] = rate_pct

                # --- Market Regime evaluation ---
                self._evaluate_regime(coin, d)

    # ==================== MARKET REGIME EVALUATOR ====================

    def _evaluate_regime(self, coin, d):
        """Evaluate market regime (trending/choppy/sideways) for a coin."""
        import time as _t
        rg = d['regime']
        now = _t.time()
        mark_px = d.get('mark_px', 0)
        if mark_px <= 0:
            return

        # Track price every poll (~15s)
        rg['price_history'].append({'time': now, 'price': mark_px})
        # Keep last 60 min
        cutoff = now - 3600
        rg['price_history'] = [p for p in rg['price_history'] if p['time'] > cutoff]

        if len(rg['price_history']) < 2:
            return

        # --- Factor 1: Price Range (0-40 pts) ---
        thirty_min_ago = now - 1800
        recent_prices = [p['price'] for p in rg['price_history'] if p['time'] > thirty_min_ago]
        if not recent_prices:
            return
        min_px = min(recent_prices)
        max_px = max(recent_prices)
        range_pct = ((max_px - min_px) / min_px * 100) if min_px > 0 else 0

        thresholds = {'BTC': 0.15, 'ETH': 0.20, 'SOL': 0.35, 'XRP': 0.30, 'PAXG': 0.08}
        flat_range = thresholds.get(coin, 0.20)
        trend_range = flat_range * 2
        if range_pct >= trend_range:
            range_score = 40
        elif range_pct >= flat_range:
            range_score = round(15 + (range_pct - flat_range) / (trend_range - flat_range) * 25)
        else:
            range_score = round((range_pct / flat_range) * 15) if flat_range > 0 else 0
        rg['range_score'] = range_score

        # --- Factor 2: Volume vs Average (0-30 pts) ---
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

        # --- Factor 3: CVD direction (0-15 pts) ---
        cvd_score = 7
        total_buy = d.get('total_buy_vol', 0)   # correct key
        total_sell = d.get('total_sell_vol', 0)  # correct key
        if total_buy + total_sell > 0:
            cvd = total_buy - total_sell
            total_vol = total_buy + total_sell
            cvd_ratio = abs(cvd) / total_vol
            if cvd_ratio >= 0.15:
                cvd_score = 15
            elif cvd_ratio >= 0.05:
                cvd_score = round(7 + (cvd_ratio - 0.05) / 0.10 * 8)
            else:
                cvd_score = round(cvd_ratio / 0.05 * 7) if 0.05 > 0 else 0
        rg['cvd_score'] = cvd_score

        # --- Factor 4: Buy/Sell Balance (0-15 pts) ---
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
                    balance_score = round(imbalance / 0.1 * 7) if 0.1 > 0 else 0
        rg['balance_score'] = balance_score

        # --- Final Score ---
        raw_score = range_score + volume_score + cvd_score + balance_score

        # --- Hysteresis + Hold Time ---
        current_label = rg['label']
        hold_min = 900  # 15 min
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

        # EMA smoothing
        rg['score'] = round(rg['score'] * 0.7 + raw_score * 0.3)

    # ==================== SNAPSHOT ENGINE (5 min) ====================

    def _run_snapshots(self):
        time.sleep(300)  # Wait 5 min
        while self.running:
            try:
                self._take_snapshot()
                self._evaluate_absorption()
            except Exception as e:
                print(f"[Snapshot] Error: {e}")
            time.sleep(300)

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

                # Save volume bucket
                d['volume_buckets'].append({
                    'buy': d['current_bucket_buy'],
                    'sell': d['current_bucket_sell'],
                    'total': d['current_bucket_buy'] + d['current_bucket_sell'],
                })
                d['current_bucket_buy'] = 0.0
                d['current_bucket_sell'] = 0.0

        print(f"[Snapshot] Taken at {datetime.now().strftime('%H:%M:%S')}")

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

                c1 = imbalance >= 60 and total_vol > 50000
                c2 = False
                if c1:
                    c2 = (flow_is_buy and price_delta <= 0.02) or (not flow_is_buy and price_delta >= -0.02)
                c3 = oi_delta > 0.05
                c4 = (flow_is_buy and funding > 0.000005) or (not flow_is_buy and funding < -0.000005)

                d['abs_conditions'] = {'flow': c1, 'reversal': c2, 'oi': c3, 'funding': c4}
                met = sum([c1, c2, c3, c4])
                d['abs_conditions_met'] = met
                d['abs_detected'] = (c1 and c2 and c3)  # Core 3 required, C4 funding is bonus
                d['abs_side'] = ('bearish' if flow_is_buy else 'bullish') if d['abs_detected'] else None

                d['signals']['absorption'] = {
                    'active': d['abs_detected'],
                    'side': d['abs_side'],
                    'detail': f"{'Sells' if d['abs_side'] == 'bullish' else 'Buys'} absorbed" if d['abs_detected'] else '',
                }

    # ==================== SIGNAL EVALUATOR (15s) ====================

    def _run_signals(self):
        time.sleep(30)  # Wait for some data
        while self.running:
            try:
                self._evaluate_all_signals()
            except Exception as e:
                print(f"[Signals] Error: {e}")
            time.sleep(15)

    def _evaluate_all_signals(self):
        now = time.time()
        with self._data_lock:
            for coin in self.coins:
                d = self.data[coin]
                sigs = d['signals']

                # 1. Absorption — already evaluated in _evaluate_absorption
                s_abs = sigs['absorption']['active']

                # 2. CVD Divergence
                s_cvd = self._check_cvd_divergence(coin)

                # 3. OI + Price Divergence
                s_oi = self._check_oi_divergence(coin)

                # 4. Volume Climax
                s_climax = self._check_volume_climax(coin)

                # 5. Funding Extreme
                s_fund = self._check_funding_extreme(coin)

                # 6. Aggressive Initiative (decays after 5 min)
                s_init = False
                if sigs['initiative']['time'] > 0:
                    if now - sigs['initiative']['time'] < 300:
                        s_init = True
                    else:
                        sigs['initiative'] = {'active': False, 'side': None, 'detail': '', 'time': 0}

                # 7. Whale Clustering (decays after 3 min)
                s_clust = False
                if sigs['clustering']['time'] > 0:
                    if now - sigs['clustering']['time'] < 180:
                        s_clust = True
                    else:
                        sigs['clustering'] = {'active': False, 'side': None, 'detail': '', 'time': 0}

                # 8. Funding Flip — stays until acknowledged
                s_flip = sigs['funding_flip']['active']

                # Count actives
                active_count = sum([s_abs, bool(s_cvd), bool(s_oi), bool(s_climax), bool(s_fund), s_flip, s_init, s_clust])
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
        """Price trending but CVD momentum weakening."""
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

        result = None
        # Bearish: price still rising but CVD momentum fading significantly
        if pd1 > 0.02 and pd2 >= 0 and cvd1 > 0 and cvd2 < cvd1 * 0.4:
            result = 'bearish'
            d['signals']['cvd_divergence'] = {
                'active': True, 'side': 'bearish',
                'detail': f"CVD fading: {cvd1/1e6:.1f}M → {cvd2/1e6:.1f}M while price still up",
            }
        # Bullish: price still falling but selling pressure fading
        elif pd1 < -0.02 and pd2 <= 0 and cvd1 < 0 and abs(cvd2) < abs(cvd1) * 0.4:
            result = 'bullish'
            d['signals']['cvd_divergence'] = {
                'active': True, 'side': 'bullish',
                'detail': f"Sell pressure fading: {cvd1/1e6:.1f}M → {cvd2/1e6:.1f}M while price still down",
            }
        else:
            d['signals']['cvd_divergence'] = {'active': False, 'side': None, 'detail': ''}

        return result

    def _check_oi_divergence(self, coin):
        """Price up but OI declining = distribution (bearish). Price down but OI declining = capitulation ending (bullish)."""
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

        result = None
        # Bearish: Price UP but OI declining = distribution
        if price_delta > 0.05 and oi_delta < -0.05:
            result = 'bearish'
            d['signals']['oi_divergence'] = {
                'active': True, 'side': 'bearish',
                'detail': f"Price +{price_delta:.2f}% but OI {oi_delta:.2f}% (distribution)",
            }
        # Bullish: Price DOWN but OI declining = shorts closing (capitulation)
        elif price_delta < -0.05 and oi_delta < -0.05:
            result = 'bullish'
            d['signals']['oi_divergence'] = {
                'active': True, 'side': 'bullish',
                'detail': f"Price {price_delta:.2f}% and OI {oi_delta:.2f}% (capitulation ending)",
            }
        else:
            d['signals']['oi_divergence'] = {'active': False, 'side': None, 'detail': ''}

        return result

    def _check_volume_climax(self, coin):
        """Current 5-min volume > 5x average = climax."""
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
        if ratio >= 5.0:
            buy_pct = d['current_bucket_buy'] / current if current > 0 else 0.5
            if buy_pct > 0.6:
                result = 'bearish'  # Buy climax often marks top
                d['signals']['volume_climax'] = {
                    'active': True, 'side': 'bearish',
                    'detail': f"Buy volume {ratio:.1f}x avg — possible blow-off top",
                }
            elif buy_pct < 0.4:
                result = 'bullish'  # Sell climax often marks bottom
                d['signals']['volume_climax'] = {
                    'active': True, 'side': 'bullish',
                    'detail': f"Sell volume {ratio:.1f}x avg — possible capitulation bottom",
                }
            else:
                d['signals']['volume_climax'] = {'active': False, 'side': None, 'detail': ''}
        else:
            d['signals']['volume_climax'] = {'active': False, 'side': None, 'detail': ''}

        return result

    def _check_funding_extreme(self, coin):
        """Detect genuinely elevated funding rates (±0.005%) indicating
        overleveraged positions. Normal funding hovers near zero and
        should NOT trigger this signal."""
        d = self.data[coin]
        funding = d['funding']
        rate_pct = funding * 100

        result = None
        if rate_pct > 0.005:
            result = 'bearish'
            d['signals']['funding_extreme'] = {
                'active': True, 'side': 'bearish',
                'detail': f"Funding +{rate_pct:.4f}% — longs overleveraged",
            }
        elif rate_pct < -0.005:
            result = 'bullish'
            d['signals']['funding_extreme'] = {
                'active': True, 'side': 'bullish',
                'detail': f"Funding {rate_pct:.4f}% — shorts overleveraged",
            }
        else:
            d['signals']['funding_extreme'] = {'active': False, 'side': None, 'detail': ''}

        return result

    # ==================== PRESSURE SNAPSHOTS ====================

    def _run_pressure(self):
        while self.running:
            time.sleep(30)
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
                    'funding': d['funding'],
                    'mark_px': d['mark_px'],
                    'oracle_px': d['oracle_px'],
                    'open_interest': d['open_interest'],
                    'day_volume': d['day_volume'],
                    'last_trade_price': d['last_trade_price'],
                    'pressure_history': list(d['pressure_history']),
                    'signals': d['signals'],
                    'alert_level': d['alert_level'],
                    'alert_label': d['alert_label'],
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
                    'flip_state': d['flip_state'],
                    'regime': d['regime'],
                }
            return state
