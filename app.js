/**
 * WhaleFlow — Hyperliquid Whale Tracker Dashboard
 * Real-time whale orderbook monitoring with funding & absorption detection
 * Continuously collects data for all coins simultaneously
 *
 * ABSORPTION ENGINE v2: Hourly threshold-based detection
 * Absorption only fires when ALL conditions are met:
 *   1) Flow Imbalance > 60% in one direction
 *   2) Price is moving AGAINST the dominant flow (reversal signal)
 *   3) OI is increasing (new positions being opened and absorbed)
 *   4) Funding rate confirms the bias direction
 */

class WhaleFlowDashboard {
    constructor() {
        // State
        this.ws = null;
        this.currentCoin = 'BTC';
        this.whaleThreshold = 50000;
        this.orderbook = { bids: [], asks: [] };
        this.fundingData = null;
        this.assetIndex = -1;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 50;
        this.reconnectDelay = 2000;
        this.isConnected = false;

        // Supported coins (reduced set)
        this.coinList = ['BTC', 'ETH', 'SOL', 'PAXG', 'XRP'];

        // Per-coin persistent data store — data collects continuously for ALL coins
        this.coinDataStore = new Map();
        this.coinList.forEach(coin => this.coinDataStore.set(coin, this._newCoinData()));

        // Per-coin funding/OI snapshots from REST API (for all coins)
        this.allCoinMeta = new Map(); // coin -> { funding, oi, markPx, oraclePx, ... }

        // "Display" copies loaded from coinDataStore for current coin
        this.whaleTrades = [];
        this.totalBuyVolume = 0;
        this.totalSellVolume = 0;
        this.buyCount = 0;
        this.sellCount = 0;
        this.pressureHistory = [];
        this.lastPressureSnapshot = { buys: 0, sells: 0 };

        // DOM cache
        this.elements = {};
        this.cacheElements();

        // Init
        this.renderCoinSelector();
        this.setupEventListeners();
        this.connectWebSocket();
        this.fetchFundingData();
        this.startClock();

        // Periodic updates
        this.fundingInterval = setInterval(() => this.fetchFundingData(), 15000);
        this.pressureInterval = setInterval(() => this.recordPressureSnapshot(), 30000);

        // Absorption engine: snapshot every 5 minutes, evaluate every 15 seconds
        this.absSnapshotInterval = setInterval(() => this.takeAbsorptionSnapshot(), 300000); // 5 min
        this.absEvalInterval = setInterval(() => this.evaluateAbsorption(), 15000); // 15s

        // Reversal Radar: evaluate all signals every 15 seconds
        this.radarInterval = setInterval(() => this.evaluateReversalSignals(), 15000);

        // Load server-accumulated state if available (Streamlit deployment)
        if (window.__SERVER_STATE__) {
            this.loadServerState(window.__SERVER_STATE__);
        }
    }

    _newCoinData() {
        return {
            whaleTrades: [],
            totalBuyVolume: 0,
            totalSellVolume: 0,
            buyCount: 0,
            sellCount: 0,
            pressureHistory: [],
            lastPressureSnapshot: { buys: 0, sells: 0 },

            // Hourly absorption accumulator
            abs: {
                cumBuyVol: 0,
                cumSellVol: 0,
                lastTradePrice: 0,
                snapshots: [],
                detected: false,
                side: null,
                conditionsMet: 0,
                conditions: { flow: false, reversal: false, oi: false, funding: false },
                metrics: { cvd: 0, vol: 0, priceDelta: 0, oiDelta: 0, imbalance: 0, funding: 0 }
            },

            // Mega whales (initiative + clustering events)
            megaWhales: [],

            // 8-Signal Reversal Radar
            signals: {
                absorption:   { active: false, side: null, detail: '' },
                cvd_divergence: { active: false, side: null, detail: '' },
                oi_divergence:  { active: false, side: null, detail: '' },
                volume_climax:  { active: false, side: null, detail: '' },
                funding_extreme: { active: false, side: null, detail: '' },
                funding_flip:   { active: false, side: null, detail: '', time: 0, acknowledged: false },
                initiative:    { active: false, side: null, detail: '', time: 0 },
                clustering:    { active: false, side: null, detail: '', time: 0 },
            },
            alertLevel: 0,
            alertLabel: 'Quiet',

            // Funding Flip tracking
            flipState: {
                currentSign: null,        // current sign ('positive'/'negative')
                lastSign: null,           // previous sign before last flip
                lastSignTime: 0,          // when current sign started
                lastFundingValue: 0,
                flipHistory: [],          // [{time, from, to, rate}]
                chaosActive: false,
                chaosResolved: false,
                chaosResolvedSide: null,
                chaosResolvedTime: 0,
            },

            // Volume buckets for climax
            volumeBuckets: [],
            currentBucketBuy: 0,
            currentBucketSell: 0,

            // Market Regime detector
            regime: {
                score: 50,
                label: 'ANALYZING…',
                cssClass: '',
                lastChangeTime: 0,
                priceHistory: [],        // [{time, price}] — last 60 min of prices
                rangeScore: 0,
                volumeScore: 0,
                cvdScore: 0,
                balanceScore: 0,
            },
        };
    }

    getCoinData(coin) {
        if (!this.coinDataStore.has(coin)) {
            this.coinDataStore.set(coin, this._newCoinData());
        }
        return this.coinDataStore.get(coin);
    }

    /**
     * Seed the frontend coin data store from the backend's accumulated state.
     * Called once on init when window.__SERVER_STATE__ is available.
     * This eliminates the "warmup" period after a page refresh.
     */
    loadServerState(state) {
        if (!state || !state.coins) return;
        console.log('🔄 Loading server-accumulated state…');

        this.coinList.forEach(coin => {
            const sd = state.coins[coin];
            if (!sd) return;
            const d = this.getCoinData(coin);

            // ---- Whale trades ----
            if (Array.isArray(sd.whale_trades)) {
                d.whaleTrades = sd.whale_trades.slice(-200); // cap at 200
            }
            if (Array.isArray(sd.mega_whales)) {
                d.megaWhales = sd.mega_whales.slice(-50);
            }

            // ---- Volume totals ----
            d.totalBuyVolume  = sd.total_buy_vol  || 0;
            d.totalSellVolume = sd.total_sell_vol || 0;
            d.buyCount        = sd.buy_count       || 0;
            d.sellCount       = sd.sell_count      || 0;

            // ---- Volume buckets ----
            if (Array.isArray(sd.volume_buckets)) {
                d.volumeBuckets = sd.volume_buckets;
            }

            // ---- Pressure history ----
            if (Array.isArray(sd.pressure_history)) {
                d.pressureHistory = sd.pressure_history;
                if (d.pressureHistory.length > 0) {
                    const last = d.pressureHistory[d.pressureHistory.length - 1];
                    d.lastPressureSnapshot = { buys: last.buys || 0, sells: last.sells || 0 };
                }
            }

            // ---- Signals ----
            if (sd.signals) {
                d.signals = sd.signals;
            }
            if (sd.alert_level !== undefined) d.alertLevel = sd.alert_level;
            if (sd.alert_label !== undefined) d.alertLabel = sd.alert_label;

            // ---- Absorption ----
            if (sd.abs) {
                const a = sd.abs;
                d.abs.detected       = a.detected  || false;
                d.abs.side           = a.side       || null;
                d.abs.conditionsMet  = a.conditions_met || 0;
                d.abs.conditions     = a.conditions || d.abs.conditions;
                d.abs.metrics        = a.metrics    || d.abs.metrics;
                d.abs.cumBuyVol      = a.cum_buy    || 0;
                d.abs.cumSellVol     = a.cum_sell   || 0;
                if (Array.isArray(a.snapshots)) {
                    d.abs.snapshots = a.snapshots;
                }
            }

            // ---- Flip state ----
            if (sd.flip_state) {
                const fs = sd.flip_state;
                d.flipState = {
                    currentSign:         fs.current_sign         || null,
                    lastSign:            fs.last_sign            || null,
                    lastSignTime:        (fs.last_sign_time      || 0) * 1000, // convert to ms
                    lastFundingValue:    fs.last_funding_value   || 0,
                    flipHistory:         (fs.flip_history        || []).map(f => ({
                        from: f.from, to: f.to,
                        time: (f.time || 0) * 1000,
                        rate: f.rate
                    })),
                    chaosActive:         fs.chaos_active         || false,
                    chaosResolved:       fs.chaos_resolved       || false,
                    chaosResolvedSide:   fs.chaos_resolved_side  || null,
                    chaosResolvedTime:  (fs.chaos_resolved_time  || 0) * 1000,
                };
            }

            // ---- Market Regime (most important — eliminates warmup completely) ----
            if (sd.regime) {
                const r = sd.regime;
                d.regime.score          = r.score       || 50;
                d.regime.label          = r.label       || 'ANALYZING…';
                d.regime.cssClass       = r.css_class   || '';
                d.regime.lastChangeTime = (r.last_change_time || 0) * 1000; // backend uses seconds
                d.regime.rangeScore     = r.range_score  || 0;
                d.regime.volumeScore    = r.volume_score || 0;
                d.regime.cvdScore       = r.cvd_score    || 0;
                d.regime.balanceScore   = r.balance_score || 0;
                // Seed price history from backend (backend uses seconds, convert to ms)
                if (Array.isArray(r.price_history)) {
                    d.regime.priceHistory = r.price_history.map(p => ({
                        time:  p.time * 1000,
                        price: p.price
                    }));
                }
            }
        });

        console.log('✅ Server state loaded — regime and flip state pre-seeded.');
        // Trigger an immediate UI render with loaded data
        this.renderRegime();
        this.renderReversalRadar();
        this.renderAbsorptionUI();
        this.renderMegaWhales();
    }

    /** Load display copies from the per-coin store */
    loadCoinData(coin) {
        const d = this.getCoinData(coin);
        this.whaleTrades = d.whaleTrades;
        this.totalBuyVolume = d.totalBuyVolume;
        this.totalSellVolume = d.totalSellVolume;
        this.buyCount = d.buyCount;
        this.sellCount = d.sellCount;
        this.pressureHistory = d.pressureHistory;
        this.lastPressureSnapshot = d.lastPressureSnapshot;
    }

    // ==================== DOM CACHING ====================

    cacheElements() {
        const ids = [
            'connectionStatus', 'coinSelector', 'whaleThreshold', 'liveClock',
            'totalWhaleBuys', 'whaleBuyCount', 'buyBarFill',
            'totalWhaleSells', 'whaleSellCount', 'sellBarFill',
            'vsCircle', 'winningBadge', 'winningArrow', 'winningLabel', 'dominancePct',
            'fundingRate', 'fundingDirection', 'absorptionStatus', 'fundingCard',
            'markPrice', 'oraclePrice', 'openInterest', 'dayVolume',
            'obAsks', 'obBids', 'obLevels', 'whaleWallCount',
            'spreadValue', 'spreadPct',
            'tradesList', 'tradeCount', 'clearTrades',
            'imbalanceRatio',
            'obBuyWalls', 'obSellWalls', 'obBuyFill', 'obSellFill',
            'tradeBuyVol', 'tradeSellVol', 'tradeBuyFill', 'tradeSellFill',
            'cvdValue', 'cvdFill',
            'pressureChart',

            // Absorption detection elements
            'absStatusBanner', 'absStatusIcon', 'absStatusLabel', 'absStatusSub',
            'absEvidencePanel',
            'evFlow', 'evPrice', 'evOI', 'evFunding', 'evCVD', 'evVolume',
            'condFlowDot', 'condFlowVal', 'condFlow',
            'condReversalDot', 'condReversalVal', 'condReversal',
            'condOIDot', 'condOIVal', 'condOI',
            'condFundingDot', 'condFundingVal', 'condFunding',
            'condProgressLabel', 'condProgressFill',
            'metricCVD', 'metricVol', 'metricPriceDelta', 'metricOIDelta', 'metricImbalance', 'metricFunding',
            'absExplanation',

            // Reversal Radar elements
            'radarAlertBadge', 'radarAlertBanner', 'radarAlertLevel',
            'alertLevelIcon', 'alertLevelText', 'radarAlertSub', 'radarAlertFill',
            'sigAbsorption', 'sigAbsorptionDot', 'sigAbsorptionDetail',
            'sigCVD', 'sigCVDDot', 'sigCVDDetail',
            'sigOI', 'sigOIDot', 'sigOIDetail',
            'sigClimax', 'sigClimaxDot', 'sigClimaxDetail',
            'sigFunding', 'sigFundingDot', 'sigFundingDetail',
            'sigInitiative', 'sigInitiativeDot', 'sigInitiativeDetail',
            'sigClustering', 'sigClusteringDot', 'sigClusteringDetail',
            'sigFlip', 'sigFlipDot', 'sigFlipDetail', 'sigFlipAck',
            'flipChaosBar', 'flipChaosText', 'flipChaosLevel', 'chaosAck',

            // Market Regime
            'regimeBar', 'regimeDot', 'regimeLabel',
            'regimeScoreFill', 'regimeScoreValue',
            'regCondRange', 'regCondVolume', 'regCondCVD', 'regCondBalance',

            // Mega Whales
            'megaWhaleCount', 'megaWhaleList'
        ];
        ids.forEach(id => {
            this.elements[id] = document.getElementById(id);
        });
    }

    // ==================== COIN SELECTOR ====================

    renderCoinSelector() {
        const container = this.elements.coinSelector;
        container.innerHTML = '';
        this.coinList.forEach(coin => {
            const btn = document.createElement('button');
            btn.className = `coin-btn${coin === this.currentCoin ? ' active' : ''}`;
            btn.textContent = coin;
            btn.dataset.coin = coin;
            btn.id = `coin-btn-${coin}`;
            container.appendChild(btn);
        });
    }

    // ==================== EVENT LISTENERS ====================

    setupEventListeners() {
        // Coin selector
        this.elements.coinSelector.addEventListener('click', (e) => {
            const btn = e.target.closest('.coin-btn');
            if (!btn || btn.dataset.coin === this.currentCoin) return;
            this.switchCoin(btn.dataset.coin);
        });

        // Whale threshold
        this.elements.whaleThreshold.addEventListener('change', (e) => {
            const val = parseInt(e.target.value, 10);
            if (val >= 1000) {
                this.whaleThreshold = val;
                this.reprocessTrades();
                this.renderOrderbook();
                this.showToast(`🐋 Whale threshold set to $${this.formatCompact(val)}`);
            }
        });

        // Clear trades
        this.elements.clearTrades.addEventListener('click', () => {
            const d = this.getCoinData(this.currentCoin);
            d.whaleTrades = [];
            d.totalBuyVolume = 0;
            d.totalSellVolume = 0;
            d.buyCount = 0;
            d.sellCount = 0;
            d.pressureHistory = [];
            d.lastPressureSnapshot = { buys: 0, sells: 0 };
            this.loadCoinData(this.currentCoin);
            this.updateSummaryCards();
            this.renderTradesList();
            this.updateAnalytics();
            this.showToast('🗑️ Trade history cleared');
        });

        // Funding Flip acknowledge button
        if (this.elements.sigFlipAck) {
            this.elements.sigFlipAck.addEventListener('click', () => {
                const d = this.getCoinData(this.currentCoin);
                d.signals.funding_flip.acknowledged = true;
                d.signals.funding_flip.active = false;
                // Store in localStorage for persistence across refreshes
                const acked = JSON.parse(localStorage.getItem('acked_flips') || '[]');
                acked.push(d.signals.funding_flip.time);
                localStorage.setItem('acked_flips', JSON.stringify(acked.slice(-50)));
                this.renderReversalRadar();
                this.showToast('✓ Funding flip acknowledged');
            });
        }

        // Chaos resolution acknowledge button
        if (this.elements.chaosAck) {
            this.elements.chaosAck.addEventListener('click', () => {
                const d = this.getCoinData(this.currentCoin);
                d.flipState.chaosResolved = false;
                d.flipState.chaosActive = false;
                d.flipState.chaosResolvedSide = null;
                d.flipState.chaosResolvedTime = 0;
                d.flipState.flipHistory = [];
                this.renderReversalRadar();
                this.showToast('✓ Chaos resolution acknowledged');
            });
        }
    }

    // ==================== WEBSOCKET ====================

    connectWebSocket() {
        if (this.ws) {
            this.ws.onclose = null;
            this.ws.onerror = null;
            this.ws.close();
            this.ws = null;
        }

        if (this._pingInterval) {
            clearInterval(this._pingInterval);
            this._pingInterval = null;
        }

        this.updateConnectionStatus('connecting');

        try {
            this.ws = new WebSocket('wss://api.hyperliquid.xyz/ws');
        } catch (err) {
            console.error('WebSocket creation failed:', err);
            this.scheduleReconnect();
            return;
        }

        this.ws.onopen = () => {
            console.log('✅ WebSocket connected');
            this.isConnected = true;
            this.reconnectAttempts = 0;
            this.updateConnectionStatus('connected');
            this.subscribeAll();

            this._pingInterval = setInterval(() => {
                if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                    this.ws.send(JSON.stringify({ method: 'ping' }));
                }
            }, 20000);
        };

        this.ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                if (msg.channel === 'pong') return;
                this.handleMessage(msg);
            } catch (err) { /* ignore non-JSON */ }
        };

        this.ws.onerror = (err) => {
            console.error('WebSocket error:', err);
        };

        this.ws.onclose = (event) => {
            console.log('WebSocket closed:', event.code, event.reason);
            this.isConnected = false;
            this.updateConnectionStatus('disconnected');
            if (this._pingInterval) {
                clearInterval(this._pingInterval);
                this._pingInterval = null;
            }
            this.scheduleReconnect();
        };
    }

    subscribeAll() {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

        // Subscribe to trades for EVERY coin — continuous collection
        this.coinList.forEach(coin => {
            this.ws.send(JSON.stringify({
                method: 'subscribe',
                subscription: { type: 'trades', coin }
            }));
        });

        // L2 orderbook only for the active coin
        this.ws.send(JSON.stringify({
            method: 'subscribe',
            subscription: { type: 'l2Book', coin: this.currentCoin }
        }));
    }

    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error('Max reconnect attempts reached');
            this.updateConnectionStatus('disconnected');
            return;
        }

        this.reconnectAttempts++;
        const delay = Math.min(this.reconnectDelay * Math.pow(1.3, this.reconnectAttempts), 30000);
        console.log(`Reconnecting in ${(delay / 1000).toFixed(1)}s (attempt ${this.reconnectAttempts})`);
        setTimeout(() => this.connectWebSocket(), delay);
    }

    switchCoin(newCoin) {
        const oldCoin = this.currentCoin;

        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                method: 'unsubscribe',
                subscription: { type: 'l2Book', coin: oldCoin }
            }));
        }

        this.currentCoin = newCoin;
        this.orderbook = { bids: [], asks: [] };
        this.fundingData = null;

        this.loadCoinData(newCoin);

        document.querySelectorAll('.coin-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.coin === newCoin);
        });

        this.elements.markPrice.textContent = '$—';
        this.elements.oraclePrice.textContent = '$—';
        this.elements.openInterest.textContent = '$—';
        this.elements.dayVolume.textContent = '$—';
        this.elements.fundingRate.textContent = '—';

        this.updateSummaryCards();
        this.renderTradesList();
        this.renderOrderbook();
        this.updateAnalytics();

        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                method: 'subscribe',
                subscription: { type: 'l2Book', coin: newCoin }
            }));
        }
        this.fetchFundingData();
        this.renderAbsorptionUI();
        this.renderReversalRadar();
        this.renderMegaWhales();
        this.renderRegime();

        this.showToast(`🔄 Switched to ${newCoin}`);
    }

    // ==================== MESSAGE HANDLING ====================

    handleMessage(msg) {
        if (!msg.channel || !msg.data) return;

        switch (msg.channel) {
            case 'trades':
                this.handleTrades(msg.data);
                break;
            case 'l2Book':
                this.handleL2Book(msg.data);
                break;
        }
    }

    handleTrades(trades) {
        if (!Array.isArray(trades)) return;

        let currentCoinUpdated = false;

        trades.forEach(trade => {
            const coin = trade.coin || this.currentCoin;
            const price = parseFloat(trade.px);
            const size = parseFloat(trade.sz);
            const value = price * size;
            const isBuy = trade.side === 'B';

            const d = this.getCoinData(coin);

            // ——— Absorption accumulator (ALL trades) ———
            if (isBuy) {
                d.abs.cumBuyVol += value;
                d.currentBucketBuy += value;
            } else {
                d.abs.cumSellVol += value;
                d.currentBucketSell += value;
            }
            d.abs.lastTradePrice = price;

            // ——— Whale tracking ———
            if (value >= this.whaleThreshold) {
                d.whaleTrades.unshift({
                    time: trade.time,
                    side: isBuy ? 'BUY' : 'SELL',
                    price, size, value, coin
                });

                if (d.whaleTrades.length > 500) {
                    d.whaleTrades = d.whaleTrades.slice(0, 500);
                }

                if (isBuy) {
                    d.totalBuyVolume += value;
                    d.buyCount++;
                } else {
                    d.totalSellVolume += value;
                    d.sellCount++;
                }

                if (coin === this.currentCoin) {
                    currentCoinUpdated = true;
                }

                // ——— Aggressive Initiative detection ———
                const megaThresholds = { BTC: 2000000, ETH: 1000000, SOL: 500000, PAXG: 200000, XRP: 300000 };
                const megaThresh = megaThresholds[coin] || 1000000;
                if (value >= megaThresh) {
                    const side = isBuy ? 'bullish' : 'bearish';
                    d.signals.initiative = {
                        active: true, side,
                        detail: `${isBuy ? 'BUY' : 'SELL'} $${(value/1e6).toFixed(2)}M @ ${price.toLocaleString()}`,
                        time: Date.now()
                    };
                    d.megaWhales.unshift({
                        time: trade.time, side: isBuy ? 'BUY' : 'SELL',
                        price, size, value, coin, mega_type: 'initiative'
                    });
                    if (d.megaWhales.length > 100) d.megaWhales = d.megaWhales.slice(0, 100);
                    if (coin === this.currentCoin) {
                        this.renderMegaWhales();
                        this.showToast(`🐋 INITIATIVE: $${(value/1e6).toFixed(2)}M ${isBuy ? 'BUY' : 'SELL'} on ${coin}!`);
                    }
                }

                // ——— Whale Clustering detection ———
                const now = Date.now();
                const recent = d.whaleTrades.filter(t => t.time >= now - 60000).slice(0, 30);
                const sameSide = recent.filter(t => t.side === (isBuy ? 'BUY' : 'SELL'));
                if (sameSide.length >= 5) {
                    const clusterVal = sameSide.reduce((s, t) => s + t.value, 0);
                    d.signals.clustering = {
                        active: true,
                        side: isBuy ? 'bullish' : 'bearish',
                        detail: `${sameSide.length} ${isBuy ? 'BUY' : 'SELL'} trades ($${(clusterVal/1e6).toFixed(2)}M) in 60s`,
                        time: Date.now()
                    };
                    // Avoid duplicate mega entries for same cluster
                    const lastMega = d.megaWhales[0];
                    if (!lastMega || lastMega.mega_type !== 'clustering' || now - lastMega.time > 60000) {
                        d.megaWhales.unshift({
                            time: now, side: isBuy ? 'BUY' : 'SELL',
                            price, size: sameSide.reduce((s,t) => s + t.size, 0),
                            value: clusterVal, coin, mega_type: 'clustering',
                            cluster_count: sameSide.length
                        });
                        if (d.megaWhales.length > 100) d.megaWhales = d.megaWhales.slice(0, 100);
                        if (coin === this.currentCoin) {
                            this.renderMegaWhales();
                            this.showToast(`🦈 CLUSTER: ${sameSide.length} ${isBuy ? 'BUY' : 'SELL'} whales on ${coin}!`);
                        }
                    }
                }
            }
        });

        if (currentCoinUpdated) {
            this.loadCoinData(this.currentCoin);
            this.updateSummaryCards();
            this.renderTradesList();
            this.updateAnalytics();
        }
    }

    handleL2Book(data) {
        if (!data.levels || data.levels.length < 2) return;

        const bids = data.levels[0].map(level => ({
            price: parseFloat(level.px),
            size: parseFloat(level.sz),
            count: level.n
        }));

        const asks = data.levels[1].map(level => ({
            price: parseFloat(level.px),
            size: parseFloat(level.sz),
            count: level.n
        }));

        this.orderbook = { bids, asks };
        this.renderOrderbook();
        this.updateAnalytics();
    }

    // ==================== FUNDING DATA ====================

    async fetchFundingData() {
        try {
            const response = await fetch('https://api.hyperliquid.xyz/info', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ type: 'metaAndAssetCtxs' })
            });

            if (!response.ok) throw new Error(`HTTP ${response.status}`);

            const data = await response.json();
            if (!Array.isArray(data) || data.length < 2) return;

            const meta = data[0];
            const contexts = data[1];
            const universe = meta.universe || [];

            // Store meta for ALL tracked coins (funding, OI, price)
            this.coinList.forEach(coin => {
                const i = universe.findIndex(u => u.name === coin);
                if (i !== -1 && contexts[i]) {
                    const ctx = contexts[i];
                    this.allCoinMeta.set(coin, {
                        funding: parseFloat(ctx.funding || '0'),
                        openInterest: parseFloat(ctx.openInterest || '0'),
                        markPx: parseFloat(ctx.markPx || '0'),
                        oraclePx: parseFloat(ctx.oraclePx || '0'),
                        dayNtlVlm: parseFloat(ctx.dayNtlVlm || '0'),
                        premium: parseFloat(ctx.premium || '0'),
                    });
                }
            });

            // Set active coin's funding
            const activeMeta = this.allCoinMeta.get(this.currentCoin);
            if (activeMeta) {
                this.fundingData = { ...activeMeta };
                this.updateFundingUI();
                this.updateMarketDataUI();
            }

            // ——— Funding Flip detection for each coin ———
            const now = Date.now();
            this.coinList.forEach(coin => {
                const meta = this.allCoinMeta.get(coin);
                if (!meta) return;
                const d = this.getCoinData(coin);
                const fs = d.flipState;
                const funding = meta.funding;
                const ratePct = funding * 100;
                const currentSign = funding >= 0 ? 'positive' : 'negative';

                // Initialize on first poll
                if (fs.lastSign === null) {
                    fs.lastSign = currentSign;
                    fs.lastSignTime = now;
                    fs.lastFundingValue = ratePct;
                    return;
                }

                // Detect flip: sign changed AND previous was above noise threshold
                if (currentSign !== fs.lastSign && Math.abs(fs.lastFundingValue) >= 0.005) {
                    const acked = JSON.parse(localStorage.getItem('acked_flips') || '[]');
                    fs.flipHistory.push({
                        time: now,
                        from: fs.lastSign,
                        to: currentSign,
                        fromRate: fs.lastFundingValue,
                        toRate: ratePct,
                    });
                    // Keep last 20 flips
                    if (fs.flipHistory.length > 20) fs.flipHistory = fs.flipHistory.slice(-20);

                    // Reset chaos resolution since we just flipped again
                    fs.chaosResolved = false;
                    fs.chaosResolvedSide = null;
                    fs.chaosResolvedTime = 0;

                    // Set signal (only if not already acknowledged)
                    if (!acked.includes(now)) {
                        d.signals.funding_flip = {
                            active: true,
                            side: currentSign === 'negative' ? 'bullish' : 'bearish',
                            detail: `Flipped ${fs.lastSign} → ${currentSign} (${fs.lastFundingValue.toFixed(4)}% → ${ratePct.toFixed(4)}%)`,
                            time: now,
                            acknowledged: false,
                        };
                        if (coin === this.currentCoin) {
                            this.showToast(`🔄 FUNDING FLIP on ${coin}: ${fs.lastSign} → ${currentSign}`);
                        }
                    }

                    fs.lastSign = currentSign;
                    fs.lastSignTime = now;
                } else if (currentSign === fs.lastSign) {
                    // Check for chaos resolution: same sign for 2 hours
                    // Use total flip history (not just last hour — flips age out of 1h window before 2h stability is reached)
                    if (fs.flipHistory.length > 0 && !fs.chaosResolved && now - fs.lastSignTime >= 7200000) {
                        fs.chaosResolved = true;
                        // Positive funding = bulls in control, Negative = bears in control
                        fs.chaosResolvedSide = currentSign === 'positive' ? 'bullish' : 'bearish';
                        fs.chaosResolvedTime = now;
                        fs.chaosActive = false;
                        if (coin === this.currentCoin) {
                            this.showToast(`✅ Funding chaos ended on ${coin} — ${fs.chaosResolvedSide.toUpperCase()} side chosen`);
                        }
                    }
                }

                // Chaos stays active as long as there are flips and no resolution
                fs.chaosActive = fs.flipHistory.length > 0 && !fs.chaosResolved;

                fs.lastFundingValue = ratePct;
            });

            // ——— Market Regime evaluation for each coin ———
            this.coinList.forEach(coin => {
                const meta = this.allCoinMeta.get(coin);
                if (!meta || !meta.markPx) return;
                const d = this.getCoinData(coin);
                const rg = d.regime;
                const now = Date.now();

                // Track price every poll (every ~15s)
                rg.priceHistory.push({ time: now, price: meta.markPx });
                // Keep last 60 minutes of prices
                const cutoff = now - 3600000;
                rg.priceHistory = rg.priceHistory.filter(p => p.time > cutoff);

                // Need at least 30s of data (2 samples at 15s intervals)
                if (rg.priceHistory.length < 2) return;

                // --- Factor 1: Price Range (0-40 pts) ---
                // Use last 30 min of prices for range calculation
                const thirtyMinAgo = now - 1800000;
                const recentPrices = rg.priceHistory.filter(p => p.time > thirtyMinAgo).map(p => p.price);
                const minPrice = Math.min(...recentPrices);
                const maxPrice = Math.max(...recentPrices);
                const rangePct = minPrice > 0 ? ((maxPrice - minPrice) / minPrice) * 100 : 0;

                // Thresholds vary by coin
                const rangeThresholds = { BTC: 0.15, ETH: 0.20, SOL: 0.35, XRP: 0.30, PAXG: 0.08 };
                const trendRange = (rangeThresholds[coin] || 0.20) * 2;   // Double = strong trend
                const flatRange = rangeThresholds[coin] || 0.20;
                let rangeScore;
                if (rangePct >= trendRange) rangeScore = 40;
                else if (rangePct >= flatRange) rangeScore = Math.round(15 + (rangePct - flatRange) / (trendRange - flatRange) * 25);
                else rangeScore = Math.round((rangePct / flatRange) * 15);
                rg.rangeScore = rangeScore;

                // --- Factor 2: Volume vs Average (0-30 pts) ---
                const buckets = d.volumeBuckets;
                let volumeScore = 15; // default mid
                if (buckets.length >= 3) {
                    const avgVol = buckets.reduce((s, b) => s + b.total, 0) / buckets.length;
                    const currentVol = d.currentBucketBuy + d.currentBucketSell;
                    if (avgVol > 0) {
                        const ratio = currentVol / avgVol;
                        if (ratio >= 2.0) volumeScore = 30;
                        else if (ratio >= 1.0) volumeScore = Math.round(15 + (ratio - 1) * 15);
                        else if (ratio >= 0.4) volumeScore = Math.round((ratio - 0.4) / 0.6 * 15);
                        else volumeScore = 0;
                    }
                }
                rg.volumeScore = volumeScore;

                // --- Factor 3: CVD direction (0-15 pts) ---
                let cvdScore = 7; // default mid
                if (d.totalBuyVolume + d.totalSellVolume > 0) {
                    const cvd = d.totalBuyVolume - d.totalSellVolume;
                    const totalVol = d.totalBuyVolume + d.totalSellVolume;
                    const cvdRatio = Math.abs(cvd) / totalVol;
                    // Strong directional CVD = trending
                    if (cvdRatio >= 0.15) cvdScore = 15;
                    else if (cvdRatio >= 0.05) cvdScore = Math.round(7 + (cvdRatio - 0.05) / 0.10 * 8);
                    else cvdScore = Math.round(cvdRatio / 0.05 * 7);
                }
                rg.cvdScore = cvdScore;

                // --- Factor 4: Buy/Sell Balance (0-15 pts) ---
                let balanceScore = 7; // default mid
                if (d.pressureHistory.length >= 3) {
                    const recent = d.pressureHistory.slice(-6);
                    const totalBuys = recent.reduce((s, p) => s + p.buys, 0);
                    const totalSells = recent.reduce((s, p) => s + p.sells, 0);
                    const total = totalBuys + totalSells;
                    if (total > 0) {
                        const buyPct = totalBuys / total;
                        const imbalance = Math.abs(buyPct - 0.5) * 2; // 0 = balanced, 1 = fully one-sided
                        if (imbalance >= 0.3) balanceScore = 15;
                        else if (imbalance >= 0.1) balanceScore = Math.round(7 + (imbalance - 0.1) / 0.2 * 8);
                        else balanceScore = Math.round(imbalance / 0.1 * 7);
                    }
                }
                rg.balanceScore = balanceScore;

                // --- Final Score (0-100) ---
                const rawScore = rangeScore + volumeScore + cvdScore + balanceScore;

                // --- Hysteresis + Hold Time ---
                const currentLabel = rg.label;
                const holdMinMs = 900000; // 15 min minimum hold
                const timeSinceChange = now - rg.lastChangeTime;
                const canChange = rg.lastChangeTime === 0 || timeSinceChange >= holdMinMs;

                let newLabel, newClass;
                if (rawScore >= 60) { newLabel = '🟢 TRENDING'; newClass = 'trending'; }
                else if (rawScore >= 30) { newLabel = '🟡 CHOPPY'; newClass = 'choppy'; }
                else { newLabel = '🔴 SIDEWAYS'; newClass = 'sideways'; }

                // Apply hysteresis: need stronger signal to change regime
                if (canChange) {
                    // Add buffer zone: must clearly cross into new regime
                    const shouldChange =
                        (newClass === 'trending' && rawScore >= 65) ||
                        (newClass === 'sideways' && rawScore <= 25) ||
                        (newClass === 'choppy' && rawScore >= 35 && rawScore <= 55) ||
                        currentLabel === 'ANALYZING\u2026';

                    if (shouldChange && newLabel !== currentLabel) {
                        rg.label = newLabel;
                        rg.cssClass = newClass;
                        rg.lastChangeTime = now;
                    }
                }

                // Always update score (smooth)
                rg.score = Math.round(rg.score * 0.7 + rawScore * 0.3); // EMA smoothing
            });

            // Render regime for active coin
            this.renderRegime();

        } catch (err) {
            console.error('Funding fetch error:', err);
        }
    }

    // ==================== ABSORPTION ENGINE v2 ====================

    /**
     * Take a 5-minute snapshot for each coin.
     * Stores cumulative buy/sell, price, OI, and funding at that moment.
     * We keep max 12 snapshots = 1 hour rolling window.
     */
    takeAbsorptionSnapshot() {
        const now = Date.now();

        this.coinList.forEach(coin => {
            const d = this.getCoinData(coin);
            const meta = this.allCoinMeta.get(coin);

            const price = d.abs.lastTradePrice || (meta ? meta.markPx : 0);
            const oi = meta ? meta.openInterest * meta.markPx : 0;
            const funding = meta ? meta.funding : 0;

            d.abs.snapshots.push({
                time: now,
                cumBuy: d.abs.cumBuyVol,
                cumSell: d.abs.cumSellVol,
                price,
                oi,
                funding
            });

            // Rolling 1h window: keep last 12 snapshots (5min * 12 = 60min)
            if (d.abs.snapshots.length > 12) {
                d.abs.snapshots = d.abs.snapshots.slice(-12);
            }

            // Record volume bucket for climax detection
            d.volumeBuckets.push({
                buy: d.currentBucketBuy,
                sell: d.currentBucketSell,
                total: d.currentBucketBuy + d.currentBucketSell
            });
            if (d.volumeBuckets.length > 12) d.volumeBuckets = d.volumeBuckets.slice(-12);
            d.currentBucketBuy = 0;
            d.currentBucketSell = 0;
        });

        console.log(`📸 Absorption snapshot taken (${new Date().toLocaleTimeString()})`);
    }

    /**
     * Evaluate absorption conditions for ALL coins every 15s.
     * Uses the 1-hour rolling window of 5-min snapshots.
     *
     * ABSORPTION = ALL 4 conditions must be true:
     *   1. Flow Imbalance > 60% (strong one-sided flow)
     *   2. Price moving AGAINST the flow (reversal / absorption signature)
     *   3. OI increasing (new positions being opened = fresh capital absorbed)
     *   4. Funding confirming the crowd bias
     */
    evaluateAbsorption() {
        this.coinList.forEach(coin => {
            const d = this.getCoinData(coin);
            const abs = d.abs;
            const meta = this.allCoinMeta.get(coin);
            const snaps = abs.snapshots;

            // Need at least 2 snapshots (10 min of data) to evaluate
            if (snaps.length < 2 || !meta) {
                abs.detected = false;
                abs.conditionsMet = 0;
                abs.conditions = { flow: false, reversal: false, oi: false, funding: false };
                if (coin === this.currentCoin) this.renderAbsorptionUI();
                return;
            }

            const oldest = snaps[0];
            const newest = snaps[snaps.length - 1];

            // ---- Compute 1h rolling metrics ----
            const buyVol = newest.cumBuy - oldest.cumBuy;
            const sellVol = newest.cumSell - oldest.cumSell;
            const totalVol = buyVol + sellVol;
            const cvd = buyVol - sellVol;

            const priceNow = d.abs.lastTradePrice || meta.markPx;
            const priceStart = oldest.price || priceNow;
            const priceDelta = priceStart > 0 ? ((priceNow - priceStart) / priceStart) * 100 : 0;

            const oiNow = meta.openInterest * meta.markPx;
            const oiStart = oldest.oi || oiNow;
            const oiDelta = oiStart > 0 ? ((oiNow - oiStart) / oiStart) * 100 : 0;

            const fundingRate = meta.funding;
            const imbalancePct = totalVol > 0 ? (Math.max(buyVol, sellVol) / totalVol) * 100 : 50;
            const flowIsBuySide = buyVol > sellVol;

            // Store metrics for display
            abs.metrics = { cvd, vol: totalVol, priceDelta, oiDelta, imbalance: imbalancePct, funding: fundingRate * 100 };

            // ---- Evaluate 4 conditions ----

            // C1: Flow Imbalance > 60%
            const c1_flow = imbalancePct >= 60 && totalVol > 50000;

            // C2: Price moving AGAINST the dominant flow direction (the reversal check)
            // If flow is buy-dominant (CVD > 0), price should be FLAT or DOWN for absorption
            // If flow is sell-dominant (CVD < 0), price should be FLAT or UP for absorption
            let c2_reversal = false;
            if (c1_flow) {
                if (flowIsBuySide && priceDelta <= 0.02) {
                    c2_reversal = true; // Buys absorbed: heavy buying but price not rising
                } else if (!flowIsBuySide && priceDelta >= -0.02) {
                    c2_reversal = true; // Sells absorbed: heavy selling but price not falling
                }
            }

            // C3: OI is increasing (new positions being opened, not just churn)
            const c3_oi = oiDelta > 0.05; // OI grew by at least 0.05%

            // C4: Funding confirms the crowd's directional bias
            // Positive funding = longs paying = crowd is long = if flow is buy-dominant, funding confirms
            // Negative funding = shorts paying = crowd is short = if flow is sell-dominant, funding confirms
            let c4_funding = false;
            if (flowIsBuySide && fundingRate > 0.000005) {
                c4_funding = true; // Crowd long + buying flow = bias confirmed
            } else if (!flowIsBuySide && fundingRate < -0.000005) {
                c4_funding = true; // Crowd short + selling flow = bias confirmed
            }

            // ---- Final Verdict ----
            abs.conditions = {
                flow: c1_flow,
                reversal: c2_reversal,
                oi: c3_oi,
                funding: c4_funding
            };

            const metCount = [c1_flow, c2_reversal, c3_oi, c4_funding].filter(Boolean).length;
            abs.conditionsMet = metCount;

            const wasDetected = abs.detected;
            abs.detected = metCount === 4;

            if (abs.detected) {
                // Determine side
                if (flowIsBuySide) {
                    // Buy flow dominant + price flat/down = passive SELLERS absorbing buys = BEARISH absorption
                    abs.side = 'bearish';
                } else {
                    // Sell flow dominant + price flat/up = passive BUYERS absorbing sells = BULLISH absorption
                    abs.side = 'bullish';
                }
            } else {
                abs.side = null;
            }

            // Toast on first detection
            if (abs.detected && !wasDetected && coin === this.currentCoin) {
                const sideLabel = abs.side === 'bullish' ? '🟢 Bullish' : '🔴 Bearish';
                this.showToast(`🔥 ${sideLabel} Absorption Detected on ${coin}!`);
            }
        });

        // Render for current coin
        this.renderAbsorptionUI();
    }

    /**
     * Render the absorption detection UI for the current coin.
     */
    renderAbsorptionUI() {
        const d = this.getCoinData(this.currentCoin);
        const abs = d.abs;
        const m = abs.metrics;
        const c = abs.conditions;

        // ---- Status Banner ----
        const banner = this.elements.absStatusBanner;
        const icon = this.elements.absStatusIcon;
        const label = this.elements.absStatusLabel;
        const sub = this.elements.absStatusSub;

        if (!banner) return; // Elements not ready

        if (abs.detected) {
            if (abs.side === 'bullish') {
                banner.className = 'abs-status-banner detected bullish-abs';
                icon.textContent = '🟢';
                label.textContent = 'BULLISH ABSORPTION DETECTED';
                sub.textContent = 'Sells absorbed — passive buyers are holding price against aggressive selling pressure';
            } else {
                banner.className = 'abs-status-banner detected bearish-abs';
                icon.textContent = '🔴';
                label.textContent = 'BEARISH ABSORPTION DETECTED';
                sub.textContent = 'Buys absorbed — passive sellers are capping price despite aggressive buy flow';
            }
        } else if (abs.snapshots.length < 2) {
            banner.className = 'abs-status-banner';
            icon.textContent = '⏳';
            label.textContent = 'Accumulating Data…';
            sub.textContent = `${abs.snapshots.length}/2 snapshots (need ≥10 min). Snapshots taken every 5 min.`;
        } else {
            banner.className = 'abs-status-banner';
            icon.textContent = '✅';
            label.textContent = 'No Absorption';
            sub.textContent = `${abs.conditionsMet}/4 conditions met — absorption fires only when all 4 align`;
        }

        // ---- Evidence Panel (only when detected) ----
        const evPanel = this.elements.absEvidencePanel;
        if (abs.detected) {
            evPanel.style.display = '';
            const flowDir = m.cvd > 0 ? 'BUY dominant' : 'SELL dominant';
            this.elements.evFlow.textContent = flowDir;
            this.elements.evFlow.style.color = m.cvd > 0 ? 'var(--buy-primary)' : 'var(--sell-primary)';

            const priceDir = m.priceDelta > 0 ? `+${m.priceDelta.toFixed(3)}%` : `${m.priceDelta.toFixed(3)}%`;
            this.elements.evPrice.textContent = priceDir + (m.priceDelta > 0 ? ' ↑' : ' ↓');
            this.elements.evPrice.style.color = m.priceDelta > 0 ? 'var(--buy-primary)' : 'var(--sell-primary)';

            this.elements.evOI.textContent = `+${m.oiDelta.toFixed(3)}% ↑`;
            this.elements.evOI.style.color = 'var(--accent-1)';

            const fundDir = m.funding > 0 ? 'Positive (Longs Pay)' : 'Negative (Shorts Pay)';
            this.elements.evFunding.textContent = fundDir;
            this.elements.evFunding.style.color = m.funding > 0 ? 'var(--buy-primary)' : 'var(--sell-primary)';

            const cvdSign = m.cvd >= 0 ? '+' : '-';
            this.elements.evCVD.textContent = cvdSign + '$' + this.formatCompact(Math.abs(m.cvd));
            this.elements.evCVD.style.color = m.cvd >= 0 ? 'var(--buy-primary)' : 'var(--sell-primary)';

            this.elements.evVolume.textContent = '$' + this.formatCompact(m.vol);
        } else {
            evPanel.style.display = 'none';
        }

        // ---- Condition Checklist ----
        const setCondition = (key, met, valText, valColor) => {
            const dotEl = this.elements[`cond${key}Dot`];
            const valEl = this.elements[`cond${key}Val`];
            const rowEl = this.elements[`cond${key}`];

            if (!dotEl || !valEl || !rowEl) return;

            if (met) {
                dotEl.textContent = '●';
                dotEl.className = 'cond-dot active';
                rowEl.className = 'abs-cond-row met';
            } else {
                dotEl.textContent = '○';
                dotEl.className = 'cond-dot';
                rowEl.className = 'abs-cond-row';
            }

            valEl.textContent = valText;
            valEl.style.color = valColor || 'var(--text-primary)';
        };

        setCondition('Flow', c.flow,
            m.imbalance ? m.imbalance.toFixed(0) + '%' : '—',
            c.flow ? 'var(--buy-primary)' : 'var(--text-secondary)');

        setCondition('Reversal', c.reversal,
            m.priceDelta !== undefined ? (m.priceDelta >= 0 ? '+' : '') + m.priceDelta.toFixed(3) + '%' : '—',
            c.reversal ? '#ffaa00' : 'var(--text-secondary)');

        setCondition('OI', c.oi,
            m.oiDelta !== undefined ? '+' + m.oiDelta.toFixed(3) + '%' : '—',
            c.oi ? 'var(--accent-1)' : 'var(--text-secondary)');

        setCondition('Funding', c.funding,
            m.funding !== undefined ? m.funding.toFixed(4) + '%' : '—',
            c.funding ? (m.funding > 0 ? 'var(--buy-primary)' : 'var(--sell-primary)') : 'var(--text-secondary)');

        // Progress bar
        const progressLabel = this.elements.condProgressLabel;
        const progressFill = this.elements.condProgressFill;
        if (progressLabel && progressFill) {
            progressLabel.textContent = `${abs.conditionsMet} / 4 conditions met`;
            progressFill.style.width = (abs.conditionsMet / 4 * 100) + '%';
            if (abs.conditionsMet === 4) {
                progressFill.className = 'cond-progress-fill triggered';
            } else if (abs.conditionsMet >= 3) {
                progressFill.className = 'cond-progress-fill warning';
            } else {
                progressFill.className = 'cond-progress-fill';
            }
        }

        // ---- Accumulator Metrics Grid ----
        const cvdSign = m.cvd >= 0 ? '+' : '-';
        if (this.elements.metricCVD) {
            this.elements.metricCVD.textContent = cvdSign + '$' + this.formatCompact(Math.abs(m.cvd));
            this.elements.metricCVD.style.color = m.cvd >= 0 ? 'var(--buy-primary)' : 'var(--sell-primary)';
        }
        if (this.elements.metricVol) {
            this.elements.metricVol.textContent = '$' + this.formatCompact(m.vol);
        }
        if (this.elements.metricPriceDelta) {
            const pd = m.priceDelta;
            this.elements.metricPriceDelta.textContent = (pd >= 0 ? '+' : '') + pd.toFixed(3) + '%';
            this.elements.metricPriceDelta.style.color = pd >= 0 ? 'var(--buy-primary)' : 'var(--sell-primary)';
        }
        if (this.elements.metricOIDelta) {
            const od = m.oiDelta;
            this.elements.metricOIDelta.textContent = (od >= 0 ? '+' : '') + od.toFixed(3) + '%';
            this.elements.metricOIDelta.style.color = od >= 0 ? 'var(--accent-1)' : 'var(--sell-primary)';
        }
        if (this.elements.metricImbalance) {
            this.elements.metricImbalance.textContent = m.imbalance ? m.imbalance.toFixed(0) + '%' : '—';
        }
        if (this.elements.metricFunding) {
            this.elements.metricFunding.textContent = m.funding !== undefined ? m.funding.toFixed(4) + '%' : '—';
            this.elements.metricFunding.style.color = m.funding >= 0 ? 'var(--buy-primary)' : 'var(--sell-primary)';
        }

        // ---- Explanation ----
        const exp = this.elements.absExplanation;
        if (exp) {
            if (abs.detected) {
                exp.className = 'absorption-explanation absorbing';
                if (abs.side === 'bullish') {
                    exp.innerHTML = `<strong>🟢 Bullish Absorption:</strong> Heavy sell flow ($${this.formatCompact(Math.abs(m.cvd))} net sell CVD) over the last hour is being absorbed — price is NOT dropping. OI rising ${m.oiDelta.toFixed(2)}% confirms new buyer positions. Funding is negative, confirming crowd is short.`;
                } else {
                    exp.innerHTML = `<strong>🔴 Bearish Absorption:</strong> Heavy buy flow ($${this.formatCompact(Math.abs(m.cvd))} net buy CVD) over the last hour is being absorbed — price is NOT rising. OI rising ${m.oiDelta.toFixed(2)}% confirms new seller positions. Funding is positive, confirming crowd is long.`;
                }
            } else {
                exp.className = 'absorption-explanation';
                if (abs.snapshots.length < 2) {
                    exp.innerHTML = `Accumulating data — snapshots are taken every 5 minutes. Need at least 2 snapshots (10 min) before evaluation begins. Currently have ${abs.snapshots.length} snapshot(s).`;
                } else {
                    exp.innerHTML = `${abs.conditionsMet}/4 conditions met. Absorption detection requires: <strong>(1)</strong> flow imbalance &gt;60%, <strong>(2)</strong> price moving against that flow (reversal), <strong>(3)</strong> OI increasing, and <strong>(4)</strong> funding confirming crowd bias. All must align simultaneously.`;
                }
            }
        }

        // ---- Summary card mini-badge ----
        const statusEl = this.elements.absorptionStatus;
        if (statusEl) {
            const badge = statusEl.querySelector('.absorption-badge');
            if (badge) {
                if (abs.detected) {
                    badge.className = 'absorption-badge absorbing';
                    badge.textContent = abs.side === 'bullish' ? '🟢 Bullish Absorption' : '🔴 Bearish Absorption';
                } else {
                    badge.className = 'absorption-badge';
                    badge.textContent = `⚖️ Normal (${abs.conditionsMet}/4)`;
                }
            }
        }
    }

    // ==================== UI UPDATES ====================

    updateConnectionStatus(status) {
        const el = this.elements.connectionStatus;
        el.className = 'connection-status ' + status;
        const textEl = el.querySelector('.status-text');

        switch (status) {
            case 'connected': textEl.textContent = 'Live'; break;
            case 'connecting': textEl.textContent = 'Connecting...'; break;
            case 'disconnected': textEl.textContent = 'Disconnected'; break;
        }
    }

    updateSummaryCards() {
        this.elements.totalWhaleBuys.textContent = '$' + this.formatCompact(this.totalBuyVolume);
        this.elements.whaleBuyCount.textContent = `${this.buyCount} trade${this.buyCount !== 1 ? 's' : ''}`;

        this.elements.totalWhaleSells.textContent = '$' + this.formatCompact(this.totalSellVolume);
        this.elements.whaleSellCount.textContent = `${this.sellCount} trade${this.sellCount !== 1 ? 's' : ''}`;

        const total = this.totalBuyVolume + this.totalSellVolume;
        if (total > 0) {
            const buyPct = (this.totalBuyVolume / total) * 100;
            this.elements.buyBarFill.style.width = buyPct + '%';
            this.elements.sellBarFill.style.width = (100 - buyPct) + '%';
        }

        this.updateWinningSide();
    }

    updateWinningSide() {
        const badge = this.elements.winningBadge;
        const arrow = this.elements.winningArrow;
        const label = this.elements.winningLabel;
        const domPct = this.elements.dominancePct;
        const total = this.totalBuyVolume + this.totalSellVolume;

        if (total === 0) {
            badge.className = 'winning-badge';
            arrow.textContent = '⬌'; label.textContent = 'Even'; domPct.textContent = '50%';
            return;
        }

        const buyPct = (this.totalBuyVolume / total) * 100;

        if (this.totalBuyVolume > this.totalSellVolume) {
            badge.className = 'winning-badge bulls';
            arrow.textContent = '⬆'; label.textContent = 'BULLS';
            domPct.textContent = buyPct.toFixed(1) + '%';
            domPct.style.color = 'var(--buy-primary)';
        } else if (this.totalSellVolume > this.totalBuyVolume) {
            badge.className = 'winning-badge bears';
            arrow.textContent = '⬇'; label.textContent = 'BEARS';
            domPct.textContent = (100 - buyPct).toFixed(1) + '%';
            domPct.style.color = 'var(--sell-primary)';
        } else {
            badge.className = 'winning-badge';
            arrow.textContent = '⬌'; label.textContent = 'Even'; domPct.textContent = '50%';
            domPct.style.color = 'var(--accent-1)';
        }
    }

    updateFundingUI() {
        if (!this.fundingData) return;
        const rate = this.fundingData.funding;
        const direction = this.elements.fundingDirection;
        const rateEl = this.elements.fundingRate;

        rateEl.textContent = (rate * 100).toFixed(4) + '%';

        let badgeClass, badgeText;
        if (rate > 0.000005) {
            badgeClass = 'positive';
            badgeText = '⬆ Positive (Longs Pay)';
            rateEl.style.color = 'var(--buy-primary)';
        } else if (rate < -0.000005) {
            badgeClass = 'negative';
            badgeText = '⬇ Negative (Shorts Pay)';
            rateEl.style.color = 'var(--sell-primary)';
        } else {
            badgeClass = 'neutral';
            badgeText = '⬌ Neutral';
            rateEl.style.color = 'var(--accent-1)';
        }

        direction.innerHTML = `<span class="direction-badge ${badgeClass}">${badgeText}</span>`;
    }

    updateMarketDataUI() {
        if (!this.fundingData) return;
        const d = this.fundingData;
        this.elements.markPrice.textContent = '$' + this.formatPrice(d.markPx);
        this.elements.oraclePrice.textContent = '$' + this.formatPrice(d.oraclePx);
        this.elements.openInterest.textContent = '$' + this.formatCompact(d.openInterest * d.markPx);
        this.elements.dayVolume.textContent = '$' + this.formatCompact(d.dayNtlVlm);
    }

    // ==================== ORDERBOOK RENDERING ====================

    renderOrderbook() {
        const { bids, asks } = this.orderbook;
        const asksEl = this.elements.obAsks;
        const bidsEl = this.elements.obBids;

        const maxLevels = 20;
        const displayAsks = asks.slice(0, maxLevels);
        const displayBids = bids.slice(0, maxLevels);

        const allSizes = [...displayAsks, ...displayBids].map(l => l.price * l.size);
        const maxNotional = Math.max(...allSizes, 1);

        let whaleWalls = 0;

        if (displayAsks.length > 0) {
            asksEl.innerHTML = displayAsks.map(level => {
                const notional = level.price * level.size;
                const pct = (notional / maxNotional) * 100;
                const isWhale = notional >= this.whaleThreshold;
                if (isWhale) whaleWalls++;
                return `<div class="ob-row ask-row${isWhale ? ' whale-level' : ''}">
                    <div class="ob-bg" style="width: ${pct}%"></div>
                    <span class="ob-price">${this.formatPrice(level.price)}</span>
                    <span class="ob-size">${this.formatSize(level.size)}</span>
                    <span class="ob-total">${this.formatCompact(notional)}</span>
                </div>`;
            }).join('');
        } else {
            asksEl.innerHTML = '<div class="ob-empty">No ask data</div>';
        }

        if (displayBids.length > 0) {
            bidsEl.innerHTML = displayBids.map(level => {
                const notional = level.price * level.size;
                const pct = (notional / maxNotional) * 100;
                const isWhale = notional >= this.whaleThreshold;
                if (isWhale) whaleWalls++;
                return `<div class="ob-row bid-row${isWhale ? ' whale-level' : ''}">
                    <div class="ob-bg" style="width: ${pct}%"></div>
                    <span class="ob-price">${this.formatPrice(level.price)}</span>
                    <span class="ob-size">${this.formatSize(level.size)}</span>
                    <span class="ob-total">${this.formatCompact(notional)}</span>
                </div>`;
            }).join('');
        } else {
            bidsEl.innerHTML = '<div class="ob-empty">No bid data</div>';
        }

        if (asks.length > 0 && bids.length > 0) {
            const bestAsk = asks[0].price;
            const bestBid = bids[0].price;
            const spread = bestAsk - bestBid;
            const spreadPct = (spread / bestAsk) * 100;
            this.elements.spreadValue.textContent = '$' + this.formatPrice(spread);
            this.elements.spreadPct.textContent = `(${spreadPct.toFixed(4)}%)`;
        }

        const totalLevels = displayAsks.length + displayBids.length;
        this.elements.obLevels.textContent = `${totalLevels} levels`;
        this.elements.whaleWallCount.textContent = `${whaleWalls} whale wall${whaleWalls !== 1 ? 's' : ''}`;
    }

    // ==================== TRADES RENDERING ====================

    renderTradesList() {
        const container = this.elements.tradesList;

        if (this.whaleTrades.length === 0) {
            container.innerHTML = `<div class="empty-state">
                <div class="empty-icon">🐋</div>
                <p>Waiting for whale activity...</p>
                <span class="empty-sub">Trades above $${this.formatCompact(this.whaleThreshold)} will appear here</span>
            </div>`;
            this.elements.tradeCount.textContent = '0 trades';
            return;
        }

        const displayTrades = this.whaleTrades.slice(0, 100);

        container.innerHTML = displayTrades.map((trade, i) => {
            const isBuy = trade.side === 'BUY';
            const isMega = trade.value >= this.whaleThreshold * 5;
            const timeStr = this.formatTime(trade.time);

            return `<div class="trade-row ${isBuy ? 'buy-trade' : 'sell-trade'}${isMega ? ' mega-whale' : ''}" ${i === 0 ? 'style="animation: tradeSlideIn 0.4s ease"' : ''}>
                <span class="trade-time">${timeStr}</span>
                <span class="trade-side">${trade.side}</span>
                <span class="trade-price">${this.formatPrice(trade.price)}</span>
                <span class="trade-size">${this.formatSize(trade.size)}</span>
                <span class="trade-value">$${this.formatCompact(trade.value)}</span>
            </div>`;
        }).join('');

        this.elements.tradeCount.textContent = `${this.whaleTrades.length} trade${this.whaleTrades.length !== 1 ? 's' : ''}`;
    }

    // ==================== ANALYTICS ====================

    updateAnalytics() {
        this.updateImbalanceBars();
        this.updateCVD();
        this.renderPressureHistory();
    }

    updateImbalanceBars() {
        let obBuyTotal = 0, obSellTotal = 0;
        this.orderbook.bids.forEach(level => {
            const notional = level.price * level.size;
            if (notional >= this.whaleThreshold) obBuyTotal += notional;
        });
        this.orderbook.asks.forEach(level => {
            const notional = level.price * level.size;
            if (notional >= this.whaleThreshold) obSellTotal += notional;
        });

        this.elements.obBuyWalls.textContent = '$' + this.formatCompact(obBuyTotal);
        this.elements.obSellWalls.textContent = '$' + this.formatCompact(obSellTotal);

        const obTotal = obBuyTotal + obSellTotal;
        if (obTotal > 0) {
            this.elements.obBuyFill.style.width = ((obBuyTotal / obTotal) * 100) + '%';
            this.elements.obSellFill.style.width = ((obSellTotal / obTotal) * 100) + '%';
        } else {
            this.elements.obBuyFill.style.width = '50%';
            this.elements.obSellFill.style.width = '50%';
        }

        this.elements.tradeBuyVol.textContent = '$' + this.formatCompact(this.totalBuyVolume);
        this.elements.tradeSellVol.textContent = '$' + this.formatCompact(this.totalSellVolume);

        const tradeTotal = this.totalBuyVolume + this.totalSellVolume;
        if (tradeTotal > 0) {
            this.elements.tradeBuyFill.style.width = ((this.totalBuyVolume / tradeTotal) * 100) + '%';
            this.elements.tradeSellFill.style.width = ((this.totalSellVolume / tradeTotal) * 100) + '%';
        } else {
            this.elements.tradeBuyFill.style.width = '50%';
            this.elements.tradeSellFill.style.width = '50%';
        }

        this.elements.imbalanceRatio.textContent = this.buyCount > 0 || this.sellCount > 0
            ? `Buy:Sell ${this.buyCount}:${this.sellCount}` : 'Buy:Sell 0:0';
    }

    updateCVD() {
        const delta = this.totalBuyVolume - this.totalSellVolume;
        const cvdEl = this.elements.cvdValue;
        const fillEl = this.elements.cvdFill;

        const sign = delta >= 0 ? '+' : '-';
        cvdEl.textContent = sign + '$' + this.formatCompact(Math.abs(delta));
        cvdEl.className = 'cvd-value ' + (delta >= 0 ? 'positive' : 'negative');

        const total = this.totalBuyVolume + this.totalSellVolume;
        if (total > 0) {
            const pct = Math.min(Math.abs(delta) / total * 100, 50);
            fillEl.style.width = pct + '%';

            if (delta >= 0) {
                fillEl.className = 'cvd-fill positive';
                fillEl.style.left = '50%';
                fillEl.style.right = 'auto';
            } else {
                fillEl.className = 'cvd-fill negative';
                fillEl.style.right = '50%';
                fillEl.style.left = 'auto';
            }
        } else {
            fillEl.style.width = '0%';
        }
    }

    recordPressureSnapshot() {
        const d = this.getCoinData(this.currentCoin);
        const buyDelta = d.totalBuyVolume - d.lastPressureSnapshot.buys;
        const sellDelta = d.totalSellVolume - d.lastPressureSnapshot.sells;

        d.pressureHistory.push({
            time: Date.now(), buys: buyDelta, sells: sellDelta, net: buyDelta - sellDelta
        });

        if (d.pressureHistory.length > 30) {
            d.pressureHistory = d.pressureHistory.slice(-30);
        }

        d.lastPressureSnapshot = { buys: d.totalBuyVolume, sells: d.totalSellVolume };
        this.loadCoinData(this.currentCoin);
        this.renderPressureHistory();
    }

    renderPressureHistory() {
        const chart = this.elements.pressureChart;

        if (this.pressureHistory.length === 0) {
            chart.innerHTML = '<div class="pressure-empty">Collecting data every 30s...</div>';
            return;
        }

        const maxVal = Math.max(...this.pressureHistory.map(p => Math.max(p.buys, p.sells)), 1);

        chart.innerHTML = this.pressureHistory.map(p => {
            const isBuy = p.net >= 0;
            const val = isBuy ? p.buys : p.sells;
            const pct = Math.max((val / maxVal) * 100, 5);
            return `<div class="pressure-bar ${isBuy ? 'buy-bar' : 'sell-bar'}" 
                         style="height: ${pct}%" 
                         data-value="$${this.formatCompact(val)}"></div>`;
        }).join('');
    }

    reprocessTrades() {
        const d = this.getCoinData(this.currentCoin);
        d.totalBuyVolume = 0; d.totalSellVolume = 0;
        d.buyCount = 0; d.sellCount = 0;

        d.whaleTrades.forEach(trade => {
            if (trade.side === 'BUY') { d.totalBuyVolume += trade.value; d.buyCount++; }
            else { d.totalSellVolume += trade.value; d.sellCount++; }
        });

        this.loadCoinData(this.currentCoin);
        this.updateSummaryCards();
        this.updateAnalytics();
    }

    // ==================== CLOCK ====================

    startClock() {
        const update = () => {
            this.elements.liveClock.textContent = new Date().toLocaleTimeString('en-US', {
                hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit'
            });
        };
        update();
        setInterval(update, 1000);
    }

    // ==================== FORMATTING ====================

    formatCompact(num) {
        if (num === 0) return '0';
        const abs = Math.abs(num);
        if (abs >= 1e9) return (num / 1e9).toFixed(2) + 'B';
        if (abs >= 1e6) return (num / 1e6).toFixed(2) + 'M';
        if (abs >= 1e3) return (num / 1e3).toFixed(1) + 'K';
        return num.toFixed(2);
    }

    formatPrice(price) {
        if (price === 0) return '0.00';
        if (price >= 10000) return price.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
        if (price >= 100) return price.toFixed(2);
        if (price >= 1) return price.toFixed(3);
        if (price >= 0.01) return price.toFixed(4);
        return price.toFixed(6);
    }

    formatSize(size) {
        if (size >= 1000) return this.formatCompact(size);
        if (size >= 1) return size.toFixed(3);
        if (size >= 0.01) return size.toFixed(4);
        return size.toFixed(6);
    }

    formatTime(timestamp) {
        return new Date(timestamp).toLocaleTimeString('en-US', {
            hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit'
        });
    }

    // ==================== SERVER STATE HYDRATION ====================

    /**
     * Load pre-accumulated state from the Python backend.
     * Called on first load when deployed on Streamlit.
     * This lets users see historical data instantly without waiting.
     */
    loadServerState(state) {
        if (!state || !state.coins) return;

        console.log(`📦 Loading server state (uptime: ${Math.round(state.uptime_seconds / 60)}min)`);

        this.coinList.forEach(coin => {
            const serverCoin = state.coins[coin];
            if (!serverCoin) return;

            const d = this.getCoinData(coin);

            // Whale trades
            if (serverCoin.whale_trades && serverCoin.whale_trades.length > 0) {
                d.whaleTrades = serverCoin.whale_trades;
                d.totalBuyVolume = serverCoin.total_buy_vol;
                d.totalSellVolume = serverCoin.total_sell_vol;
                d.buyCount = serverCoin.buy_count;
                d.sellCount = serverCoin.sell_count;
                d.lastPressureSnapshot = {
                    buys: serverCoin.total_buy_vol,
                    sells: serverCoin.total_sell_vol
                };
            }

            // Pressure history
            if (serverCoin.pressure_history && serverCoin.pressure_history.length > 0) {
                d.pressureHistory = serverCoin.pressure_history;
            }

            // Absorption state
            if (serverCoin.abs) {
                const sa = serverCoin.abs;
                d.abs.cumBuyVol = sa.cum_buy || 0;
                d.abs.cumSellVol = sa.cum_sell || 0;
                d.abs.detected = sa.detected || false;
                d.abs.side = sa.side || null;
                d.abs.conditionsMet = sa.conditions_met || 0;
                d.abs.conditions = sa.conditions || d.abs.conditions;
                d.abs.metrics = {
                    cvd: (sa.metrics && sa.metrics.cvd) || 0,
                    vol: (sa.metrics && sa.metrics.vol) || 0,
                    priceDelta: (sa.metrics && sa.metrics.price_delta) || 0,
                    oiDelta: (sa.metrics && sa.metrics.oi_delta) || 0,
                    imbalance: (sa.metrics && sa.metrics.imbalance) || 0,
                    funding: (sa.metrics && sa.metrics.funding) || 0,
                };

                // Load absorption snapshots
                if (sa.snapshots && sa.snapshots.length > 0) {
                    d.abs.snapshots = sa.snapshots.map(s => ({
                        time: s.time * 1000,
                        cumBuy: s.cum_buy,
                        cumSell: s.cum_sell,
                        price: s.price,
                        oi: s.oi,
                        funding: s.funding
                    }));
                }
            }

            // Market data
            if (serverCoin.mark_px) {
                d.abs.lastTradePrice = serverCoin.last_trade_price || serverCoin.mark_px;
            }

            // Mega whales
            if (serverCoin.mega_whales && serverCoin.mega_whales.length > 0) {
                d.megaWhales = serverCoin.mega_whales;
            }

            // Reversal Radar signals
            if (serverCoin.signals) {
                d.signals = serverCoin.signals;
            }
            if (serverCoin.alert_level !== undefined) {
                d.alertLevel = serverCoin.alert_level;
                d.alertLabel = serverCoin.alert_label || 'Quiet';
            }

            // Volume buckets
            if (serverCoin.volume_buckets) {
                d.volumeBuckets = serverCoin.volume_buckets;
            }
        });

        // Load current coin's data into display copies
        this.loadCoinData(this.currentCoin);

        // Set funding data for current coin
        const activeCoin = state.coins[this.currentCoin];
        if (activeCoin && activeCoin.mark_px) {
            this.fundingData = {
                funding: activeCoin.funding,
                openInterest: activeCoin.open_interest,
                markPx: activeCoin.mark_px,
                oraclePx: activeCoin.oracle_px,
                dayNtlVlm: activeCoin.day_volume,
            };
            this.updateFundingUI();
            this.updateMarketDataUI();
        }

        // Refresh all UI
        this.updateSummaryCards();
        this.renderTradesList();
        this.updateAnalytics();
        this.renderAbsorptionUI();
        this.renderReversalRadar();
        this.renderMegaWhales();

        const tradeCount = Object.values(state.coins).reduce((s, c) => s + (c.whale_trades ? c.whale_trades.length : 0), 0);
        this.showToast(`📦 Loaded ${tradeCount} whale trades from server (${Math.round(state.uptime_seconds / 60)}min uptime)`);
    }

    // ==================== REVERSAL RADAR ENGINE ====================

    evaluateReversalSignals() {
        const d = this.getCoinData(this.currentCoin);
        const sigs = d.signals;
        const now = Date.now();

        // 1. Absorption — already set by evaluateAbsorption
        sigs.absorption.active = d.abs.detected;
        sigs.absorption.side = d.abs.side;
        sigs.absorption.detail = d.abs.detected
            ? (d.abs.side === 'bullish' ? 'Sells absorbed' : 'Buys absorbed') : '';

        // 2. CVD Divergence
        const snaps = d.abs.snapshots;
        if (snaps.length >= 4) {
            const mid = Math.floor(snaps.length / 2);
            const s0 = snaps[0], s1 = snaps[mid], s2 = snaps[mid], s3 = snaps[snaps.length - 1];
            const p1s = s0.price, p1e = s1.price, p2s = s2.price, p2e = s3.price;
            if (p1s > 0 && p2s > 0) {
                const pd1 = ((p1e - p1s) / p1s) * 100;
                const pd2 = ((p2e - p2s) / p2s) * 100;
                const cvd1 = (s1.cumBuy - s0.cumBuy) - (s1.cumSell - s0.cumSell);
                const cvd2 = (s3.cumBuy - s2.cumBuy) - (s3.cumSell - s2.cumSell);

                if (pd1 > 0.02 && pd2 >= 0 && cvd1 > 0 && cvd2 < cvd1 * 0.4) {
                    sigs.cvd_divergence = { active: true, side: 'bearish', detail: `CVD fading: momentum down ${((1 - cvd2/cvd1)*100).toFixed(0)}%` };
                } else if (pd1 < -0.02 && pd2 <= 0 && cvd1 < 0 && Math.abs(cvd2) < Math.abs(cvd1) * 0.4) {
                    sigs.cvd_divergence = { active: true, side: 'bullish', detail: `Sell pressure fading ${((1 - Math.abs(cvd2)/Math.abs(cvd1))*100).toFixed(0)}%` };
                } else {
                    sigs.cvd_divergence = { active: false, side: null, detail: '' };
                }
            }
        }

        // 3. OI + Price Divergence
        if (snaps.length >= 3 && this.fundingData) {
            const oldest = snaps[0], newest = snaps[snaps.length - 1];
            const pDelta = ((newest.price - oldest.price) / oldest.price) * 100;
            const oiNow = (this.fundingData.openInterest || 0) * (this.fundingData.markPx || 0);
            const oiOld = oldest.oi || oiNow;
            const oiDelta = oiOld > 0 ? ((oiNow - oiOld) / oiOld) * 100 : 0;

            if (pDelta > 0.05 && oiDelta < -0.05) {
                sigs.oi_divergence = { active: true, side: 'bearish', detail: `Price +${pDelta.toFixed(2)}% but OI ${oiDelta.toFixed(2)}%` };
            } else if (pDelta < -0.05 && oiDelta < -0.05) {
                sigs.oi_divergence = { active: true, side: 'bullish', detail: `Price ${pDelta.toFixed(2)}% + OI ${oiDelta.toFixed(2)}% (capitulation)` };
            } else {
                sigs.oi_divergence = { active: false, side: null, detail: '' };
            }
        }

        // 4. Volume Climax
        if (d.volumeBuckets.length >= 3) {
            const avg = d.volumeBuckets.reduce((s, b) => s + (b.total || b.buy + b.sell), 0) / d.volumeBuckets.length;
            const current = d.currentBucketBuy + d.currentBucketSell;
            if (avg > 0 && current / avg >= 5.0) {
                const buyPct = current > 0 ? d.currentBucketBuy / current : 0.5;
                if (buyPct > 0.6) {
                    sigs.volume_climax = { active: true, side: 'bearish', detail: `Buy volume ${(current/avg).toFixed(1)}x avg — blow-off top` };
                } else if (buyPct < 0.4) {
                    sigs.volume_climax = { active: true, side: 'bullish', detail: `Sell volume ${(current/avg).toFixed(1)}x avg — capitulation` };
                } else {
                    sigs.volume_climax = { active: false, side: null, detail: '' };
                }
            } else {
                sigs.volume_climax = { active: false, side: null, detail: '' };
            }
        }

        // 5. Funding Extreme
        if (this.fundingData && this.fundingData.funding !== undefined) {
            const ratePct = this.fundingData.funding * 100;
            if (ratePct > 0.03) {
                sigs.funding_extreme = { active: true, side: 'bearish', detail: `Funding ${ratePct.toFixed(4)}% — longs overleveraged` };
            } else if (ratePct < -0.03) {
                sigs.funding_extreme = { active: true, side: 'bullish', detail: `Funding ${ratePct.toFixed(4)}% — shorts overleveraged` };
            } else {
                sigs.funding_extreme = { active: false, side: null, detail: '' };
            }
        }

        // 6. Funding Flip — only dismissed by acknowledge button, no auto-dismiss

        // 7. Initiative decay (5 min)
        if (sigs.initiative.time > 0 && now - sigs.initiative.time > 300000) {
            sigs.initiative = { active: false, side: null, detail: '', time: 0 };
        }

        // 8. Clustering decay (3 min)
        if (sigs.clustering.time > 0 && now - sigs.clustering.time > 180000) {
            sigs.clustering = { active: false, side: null, detail: '', time: 0 };
        }

        // Count active
        const activeList = [sigs.absorption, sigs.cvd_divergence, sigs.oi_divergence, sigs.volume_climax, sigs.funding_extreme, sigs.funding_flip, sigs.initiative, sigs.clustering];
        const activeCount = activeList.filter(s => s.active).length;
        d.alertLevel = Math.min(activeCount, 4);
        if (activeCount === 0) d.alertLabel = 'Quiet';
        else if (activeCount === 1) d.alertLabel = 'Watch';
        else if (activeCount <= 3) d.alertLabel = 'High Probability';
        else d.alertLabel = 'Extreme Conviction';

        this.renderReversalRadar();
    }

    // ==================== REVERSAL RADAR UI ====================

    renderReversalRadar() {
        const d = this.getCoinData(this.currentCoin);
        const sigs = d.signals;
        const level = d.alertLevel;
        const label = d.alertLabel;

        // Alert level banner
        const banner = this.elements.radarAlertBanner;
        if (banner) {
            banner.className = 'radar-alert-banner';
            if (level >= 4) banner.classList.add('level-3');
            else if (level >= 2) banner.classList.add('level-2');
            else if (level >= 1) banner.classList.add('level-1');
        }

        const icons = ['🟢', '🟡', '🟠', '🔴', '🔴'];
        const labels = ['QUIET', 'WATCH', 'HIGH PROBABILITY', 'HIGH PROBABILITY', 'EXTREME CONVICTION'];
        const subs = [
            'No reversal conditions detected',
            'Something is stirring — stay alert',
            'Multiple independent confirmations',
            'Multiple independent confirmations — be ready',
            'Extremely rare alignment — reversal imminent'
        ];

        const lvl = Math.min(level, 4);
        if (this.elements.alertLevelIcon) this.elements.alertLevelIcon.textContent = icons[lvl];
        if (this.elements.alertLevelText) this.elements.alertLevelText.textContent = labels[lvl];

        const activeCount = [sigs.absorption, sigs.cvd_divergence, sigs.oi_divergence, sigs.volume_climax, sigs.funding_extreme, sigs.funding_flip, sigs.initiative, sigs.clustering].filter(s => s.active).length;
        if (this.elements.radarAlertSub) this.elements.radarAlertSub.textContent = `${activeCount} / 8 signals active — ${subs[lvl]}`;

        const fill = this.elements.radarAlertFill;
        if (fill) {
            fill.style.width = `${(activeCount / 8) * 100}%`;
            fill.className = 'radar-alert-fill';
            if (lvl >= 4) fill.classList.add('level-3');
            else if (lvl >= 2) fill.classList.add('level-2');
            else if (lvl >= 1) fill.classList.add('level-1');
        }

        if (this.elements.radarAlertBadge) {
            this.elements.radarAlertBadge.textContent = `${icons[lvl]} ${label} (${activeCount}/8)`;
        }

        // Update each signal row
        const signalMap = [
            { key: 'absorption', row: 'sigAbsorption', dot: 'sigAbsorptionDot', detail: 'sigAbsorptionDetail' },
            { key: 'cvd_divergence', row: 'sigCVD', dot: 'sigCVDDot', detail: 'sigCVDDetail' },
            { key: 'oi_divergence', row: 'sigOI', dot: 'sigOIDot', detail: 'sigOIDetail' },
            { key: 'volume_climax', row: 'sigClimax', dot: 'sigClimaxDot', detail: 'sigClimaxDetail' },
            { key: 'funding_extreme', row: 'sigFunding', dot: 'sigFundingDot', detail: 'sigFundingDetail' },
            { key: 'funding_flip', row: 'sigFlip', dot: 'sigFlipDot', detail: 'sigFlipDetail' },
            { key: 'initiative', row: 'sigInitiative', dot: 'sigInitiativeDot', detail: 'sigInitiativeDetail' },
            { key: 'clustering', row: 'sigClustering', dot: 'sigClusteringDot', detail: 'sigClusteringDetail' },
        ];

        signalMap.forEach(({ key, row, dot, detail }) => {
            const sig = sigs[key];
            const rowEl = this.elements[row];
            const dotEl = this.elements[dot];
            const detailEl = this.elements[detail];

            if (rowEl) {
                rowEl.className = 'signal-row';
                if (sig.active) {
                    rowEl.classList.add('active');
                    if (sig.side === 'bearish') rowEl.classList.add('bearish');
                }
            }
            if (dotEl) {
                dotEl.textContent = sig.active ? '●' : '○';
                dotEl.className = 'signal-dot';
                if (sig.active) {
                    dotEl.classList.add('active');
                    if (sig.side === 'bearish') dotEl.classList.add('bearish');
                }
            }
            if (detailEl) {
                detailEl.textContent = sig.active ? sig.detail : '—';
                detailEl.style.color = sig.active
                    ? (sig.side === 'bearish' ? 'var(--sell-primary)' : 'var(--buy-primary)')
                    : 'var(--text-secondary)';
            }
        });

        // Acknowledge button visibility
        if (this.elements.sigFlipAck) {
            this.elements.sigFlipAck.style.display = sigs.funding_flip.active ? 'inline-block' : 'none';
        }

        // Chaos counter bar — stays visible once triggered, until acknowledged
        const fs = d.flipState;
        const chaosBar = this.elements.flipChaosBar;
        const chaosAckBtn = this.elements.chaosAck;
        if (chaosBar) {
            const now = Date.now();
            const flipsInHour = fs.flipHistory.filter(f => now - f.time < 3600000).length;
            const totalFlips = fs.flipHistory.length;

            if (fs.chaosResolved) {
                // Resolution: show with ack button
                chaosBar.style.display = 'flex';
                chaosBar.className = 'flip-chaos-bar resolved';
                this.elements.flipChaosText.textContent = `Chaos ended — ${fs.chaosResolvedSide} side chosen (stable 2h+)`;
                this.elements.flipChaosLevel.textContent = '✅ Resolved';
                this.elements.flipChaosLevel.className = 'chaos-level resolved';
                chaosBar.querySelector('.chaos-icon').textContent = '✅';
                if (chaosAckBtn) chaosAckBtn.style.display = 'inline-block';
            } else if (totalFlips > 0) {
                // Active chaos — stays visible continuously
                chaosBar.style.display = 'flex';
                chaosBar.className = 'flip-chaos-bar';
                this.elements.flipChaosText.textContent = `${flipsInHour} flips in 1h (${totalFlips} total) — ${flipsInHour >= 4 ? 'Major move imminent' : flipsInHour >= 2 ? 'High indecision' : 'Shift detected'}`;
                const lvlClass = flipsInHour >= 4 ? 'extreme' : 'indecision';
                this.elements.flipChaosLevel.textContent = flipsInHour >= 4 ? '💥 Extreme' : '⚡ Indecision';
                this.elements.flipChaosLevel.className = `chaos-level ${lvlClass}`;
                chaosBar.querySelector('.chaos-icon').textContent = '⚡';
                if (chaosAckBtn) chaosAckBtn.style.display = 'none';
            } else {
                chaosBar.style.display = 'none';
                if (chaosAckBtn) chaosAckBtn.style.display = 'none';
            }
        }
    }

    // ==================== MARKET REGIME RENDERER ====================

    renderRegime() {
        const d = this.getCoinData(this.currentCoin);
        const rg = d.regime;
        const bar = this.elements.regimeBar;
        if (!bar) return;

        // Update CSS class for color
        bar.className = `regime-bar ${rg.cssClass}`;

        // Label
        if (this.elements.regimeLabel) {
            this.elements.regimeLabel.textContent = rg.label;
        }

        // Score bar
        if (this.elements.regimeScoreFill) {
            this.elements.regimeScoreFill.style.width = `${rg.score}%`;
        }
        if (this.elements.regimeScoreValue) {
            this.elements.regimeScoreValue.textContent = `${rg.score}`;
        }

        // Condition pills
        const setCondition = (el, text, score, maxScore) => {
            if (!el) return;
            el.textContent = text;
            const pct = score / maxScore;
            if (pct >= 0.6) el.className = 'regime-cond bullish';
            else if (pct >= 0.3) el.className = 'regime-cond neutral';
            else el.className = 'regime-cond bearish';
        };

        // Range pill
        const recentPrices = rg.priceHistory.filter(p => Date.now() - p.time < 1800000).map(p => p.price);
        let rangePctDisplay = '—';
        if (recentPrices.length > 2) {
            const min = Math.min(...recentPrices);
            const max = Math.max(...recentPrices);
            rangePctDisplay = min > 0 ? `${((max - min) / min * 100).toFixed(3)}%` : '—';
        }
        setCondition(this.elements.regCondRange, `📏 Range: ${rangePctDisplay}`, rg.rangeScore, 40);
        setCondition(this.elements.regCondVolume, `📊 Vol: ${rg.volumeScore}/30`, rg.volumeScore, 30);
        setCondition(this.elements.regCondCVD, `⚖️ CVD: ${rg.cvdScore}/15`, rg.cvdScore, 15);
        setCondition(this.elements.regCondBalance, `🎯 Balance: ${rg.balanceScore}/15`, rg.balanceScore, 15);
    }

    // ==================== MEGA WHALES PANEL ====================

    renderMegaWhales() {
        const d = this.getCoinData(this.currentCoin);
        const list = this.elements.megaWhaleList;
        const countEl = this.elements.megaWhaleCount;
        if (!list) return;

        const megas = d.megaWhales || [];

        if (countEl) countEl.textContent = `${megas.length} events`;

        if (megas.length === 0) {
            list.innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">💎</div>
                    <p>Waiting for mega whale activity...</p>
                    <span class="empty-sub">Initiative trades ($M+) and whale clusters (5+ burst) appear here</span>
                </div>`;
            return;
        }

        list.innerHTML = megas.slice(0, 50).map(m => {
            const isBuy = m.side === 'BUY';
            const typeClass = m.mega_type === 'initiative' ? 'initiative-entry' : 'clustering-entry';
            const typeBadge = m.mega_type === 'initiative'
                ? '<span class="mega-type-badge initiative">⚡ INITIATIVE</span>'
                : `<span class="mega-type-badge clustering">🦈 CLUSTER ×${m.cluster_count || '?'}</span>`;
            const timeStr = this.formatTime(m.time);
            const valueStr = m.value >= 1e6 ? `$${(m.value/1e6).toFixed(2)}M` : `$${(m.value/1e3).toFixed(0)}K`;

            return `
                <div class="mega-whale-entry ${typeClass}">
                    <span class="mega-time">${timeStr}</span>
                    <span class="mega-side ${isBuy ? 'buy' : 'sell'}">${m.side}</span>
                    <span class="mega-info">${m.coin} @ $${m.price.toLocaleString()} ${typeBadge}</span>
                    <span class="mega-value" style="color: ${isBuy ? 'var(--buy-primary)' : 'var(--sell-primary)'}">${valueStr}</span>
                </div>`;
        }).join('');
    }

    // ==================== TOAST NOTIFICATIONS ====================

    showToast(message) {
        const existing = document.querySelector('.toast');
        if (existing) existing.remove();

        const toast = document.createElement('div');
        toast.className = 'toast';
        toast.textContent = message;
        document.body.appendChild(toast);

        setTimeout(() => toast.remove(), 4000);
    }
}

// ==================== INITIALIZE ====================
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new WhaleFlowDashboard();
});
