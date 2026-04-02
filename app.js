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
                cumBuyVol: 0,    // Running total buy volume ($) since page load
                cumSellVol: 0,   // Running total sell volume ($)
                lastTradePrice: 0,
                // 5-min snapshots for rolling 1h window (max 12 entries)
                snapshots: [],   // { time, cumBuy, cumSell, price, oi, funding }
                // Last evaluation result
                detected: false,
                side: null,      // 'bullish' or 'bearish'
                conditionsMet: 0,
                conditions: { flow: false, reversal: false, oi: false, funding: false },
                metrics: { cvd: 0, vol: 0, priceDelta: 0, oiDelta: 0, imbalance: 0, funding: 0 }
            }
        };
    }

    getCoinData(coin) {
        if (!this.coinDataStore.has(coin)) {
            this.coinDataStore.set(coin, this._newCoinData());
        }
        return this.coinDataStore.get(coin);
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
            'absExplanation'
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
        this.renderAbsorptionUI(); // Refresh absorption panel for new coin

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
            } else {
                d.abs.cumSellVol += value;
            }
            d.abs.lastTradePrice = price;

            // ——— Whale tracking ———
            if (value >= this.whaleThreshold) {
                d.whaleTrades.unshift({
                    time: trade.time,
                    side: isBuy ? 'BUY' : 'SELL',
                    price, size, value, coin
                });

                if (d.whaleTrades.length > 200) {
                    d.whaleTrades = d.whaleTrades.slice(0, 200);
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

        const tradeCount = Object.values(state.coins).reduce((s, c) => s + (c.whale_trades ? c.whale_trades.length : 0), 0);
        this.showToast(`📦 Loaded ${tradeCount} whale trades from server (${Math.round(state.uptime_seconds / 60)}min uptime)`);
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
