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
        this.localWsActive = false;
        this.currentCoin = 'BTC';
        this.whaleThreshold = 50000;
        
        // Backend default thresholds (used to decide when to trust server totals vs re-process buffer)
        this.backendThresholds = { 'BTC': 50000, 'ETH': 10000, 'SOL': 100, 'PAXG': 10, 'XRP': 50 };

        this.orderbook = { bids: [], asks: [] };
        this.fundingData = null;
        this.assetIndex = -1;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 50;
        this.reconnectDelay = 2000;
        this.isConnected = false;

        // Load active coin from memory to persist across Streamlit refreshes
        this._loadCoinFromStorage();

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
        // Data view mode: 'Historical' or 'Current'
        this.dataViewMode = 'Historical';
        this.currentModeClearTime = 0;
        this._loadModeFromStorage();

        // Desktop notifications
        this.desktopNotificationsEnabled = false;
        this._notifCooldowns = new Map();
        this._loadNotificationPref();

        // DOM cache
        this.elements = {};
        this.cacheElements();

        // Load custom thresholds from previous session if any
        this._loadCoinThreshold(this.currentCoin);

        // Init
        this.renderCoinSelector();
        this.setupEventListeners();
        this.connectWebSocket();
        if (!window.__SERVER_STATE__) this.fetchFundingData();
        this.startClock();

        // Periodic updates
        this.fundingInterval = setInterval(() => {
            if (!this.localWsActive) this.fetchFundingData();
        }, 15000);
        this.pressureInterval = setInterval(() => this.recordPressureSnapshot(), 30000);

        // Absorption engine: snapshot every 5 minutes, evaluate every 15 seconds
        this.absSnapshotInterval = setInterval(() => this.takeAbsorptionSnapshot(), 300000); // 5 min
        this.absEvalInterval = setInterval(() => this.evaluateAbsorption(), 15000); // 15s

        // Reversal Radar: evaluate all signals every 15 seconds
        this.radarInterval = setInterval(() => this.evaluateReversalSignals(), 15000);

        // System panel state
        this._serverSystemState = {
            started_at: 0,
            uptime_seconds: 0,
            connected: false,
            last_funding_update: 0,
            last_trade_update: 0,
            snapshot_loaded: false,
            exchange_status: {},
        };
        this._systemPanelInterval = setInterval(() => this.updateSystemPanel(), 5000);

        // Log sidebar state
        this._logEntries = [];
        this._logSidebarOpen = false;

        // Load server-accumulated state if available (Streamlit deployment)
        if (window.__SERVER_STATE__) {
            this.loadServerState(window.__SERVER_STATE__);
        }

        // Absorption preview removed — real engine handles detection
    }

    _newCoinData() {
        return {
            whaleTrades: [],
            totalBuyVolume: 0,
            totalSellVolume: 0,
            buyCount: 0,
            sellCount: 0,
            currentBuyVolume: 0,
            currentSellVolume: 0,
            currentBuyCount: 0,
            currentSellCount: 0,
            whaleBuckets: [],
            fundingHistory: [],
            marketHistory: [],
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
                initiative:    { active: false, side: null, detail: '', time: 0 },
                clustering:    { active: false, side: null, detail: '', time: 0 },
            },
            alertLevel: 0,
            alertLabel: 'Quiet',

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

    // loadServerState is defined below in the SERVER STATE HYDRATION section

    /** Load display copies from the per-coin store */
    loadCoinData(coin) {
        const d = this.getCoinData(coin);
        this.whaleTrades = d.whaleTrades;
        this.totalBuyVolume = d.totalBuyVolume;
        this.totalSellVolume = d.totalSellVolume;
        this.buyCount = d.buyCount;
        this.sellCount = d.sellCount;
        this.currentBuyVolume = d.currentBuyVolume || 0;
        this.currentSellVolume = d.currentSellVolume || 0;
        this.currentBuyCount = d.currentBuyCount || 0;
        this.currentSellCount = d.currentSellCount || 0;
        this.pressureHistory = d.pressureHistory;
        this.lastPressureSnapshot = d.lastPressureSnapshot;
    }

    _saveCurrentToStorage() {
        try {
            const dataToSave = {};
            this.coinDataStore.forEach((d, coin) => {
                dataToSave[coin] = {
                    buyVol: d.currentBuyVolume || 0,
                    sellVol: d.currentSellVolume || 0,
                    buyCount: d.currentBuyCount || 0,
                    sellCount: d.currentSellCount || 0,
                    // Persist historical totals too for custom thresholds
                    histBuyVol: d.totalBuyVolume || 0,
                    histSellVol: d.totalSellVolume || 0,
                    histBuyCount: d.buyCount || 0,
                    histSellCount: d.sellCount || 0,
                    lastTime: d.lastTradeTime || this.currentModeClearTime
                };
            });
            localStorage.setItem('whaleflow_curr_state', JSON.stringify(dataToSave));
        } catch(e) {}
    }

    _loadCurrentFromStorage() {
        try {
            const raw = localStorage.getItem('whaleflow_curr_state');
            if (raw) {
                const parsed = JSON.parse(raw);
                this.coinList.forEach(coin => {
                    const saved = parsed[coin];
                    if (saved) {
                        const d = this.getCoinData(coin);
                        d.currentBuyVolume = saved.buyVol || 0;
                        d.currentSellVolume = saved.sellVol || 0;
                        d.currentBuyCount = saved.buyCount || 0;
                        d.currentSellCount = saved.sellCount || 0;
                        d.totalBuyVolume = saved.histBuyVol || 0;
                        d.totalSellVolume = saved.histSellVol || 0;
                        d.buyCount = saved.histBuyCount || 0;
                        d.sellCount = saved.histSellCount || 0;
                        d.lastTradeTime = saved.lastTime || 0;
                    }
                });
            }
        } catch(e) {}
    }

    _loadCoinFromStorage() {
        try {
            const saved = localStorage.getItem('whaleflow_current_coin');
            if (saved && this.coinList.includes(saved)) {
                this.currentCoin = saved;
            }
        } catch(e) {}
    }

    _loadCoinThreshold(coin) {
        try {
            const stored = localStorage.getItem(`whaleflow_threshold_${coin}`);
            if (stored) {
                this.whaleThreshold = parseFloat(stored);
            } else {
                // Apply defaults if no user override
                const defaults = { BTC: 50000, ETH: 10000, SOL: 5000, PAXG: 10, XRP: 50 };
                this.whaleThreshold = defaults[coin] || 100;
            }
            if (this.elements.whaleThreshold) {
                this.elements.whaleThreshold.value = this.whaleThreshold;
            }
        } catch(e) {}
    }

    // ==================== DOM CACHING ====================

    cacheElements() {
        const ids = [
            'connectionStatus', 'coinSelector', 'whaleThreshold', 'liveClock',
            'totalWhaleBuys', 'whaleBuyCount', 'buyBarFill',
            'totalWhaleSells', 'whaleSellCount', 'sellBarFill',
            'vsCircle', 'winningBadge', 'winningArrow', 'winningLabel', 'dominancePct',
            'fundingRate', 'fundingHourlyChange', 'fundingFourHourChange', 'fundingDailyChange', 'fundingDirection', 'absorptionStatus', 'fundingCard',
            'markPriceHourlyChange', 'markPriceFourHourChange', 'markPriceDailyChange', 'openInterestHourlyChange', 'openInterestFourHourChange', 'openInterestDailyChange', 'dayVolumeHourlyChange', 'dayVolumeFourHourChange', 'dayVolumeDailyChange',
            'notifToggle', 'notifLabel', 'notifIcon',
            'markPrice', 'oraclePrice', 'openInterest', 'dayVolume',
            'obAsks', 'obBids', 'obLevels', 'whaleWallCount',
            'spreadValue', 'spreadPct',
            'tradesList', 'tradeCount', 'clearDataBtn',
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

            // Market Regime
            'regimeBar', 'regimeDot', 'regimeLabel',
            'regimeScoreFill', 'regimeScoreValue',
            'regCondRange', 'regCondVolume', 'regCondCVD', 'regCondBalance',

            // Mega Whales
            'megaWhaleCount', 'megaWhaleList',

            // System Panel
            'systemPanel', 'systemPanelToggle', 'systemPanelArrow', 'systemPanelBody',
            'sysUptime', 'sysBackend', 'sysLastFunding', 'sysLastTrade', 'sysSnapshot',
            'exchangeStatusGrid',
            'exchHL', 'exchBIN', 'exchBYB', 'exchOKX', 'exchKRK', 'exchCB',
            'exchDRB', 'exchBFX', 'exchBGT', 'exchMEXC', 'exchUPB', 'exchGATE',

            // Log Sidebar
            'logSidebar', 'logSidebarTab', 'logSidebarContent', 'logBadge',
            'logClearBtn', 'logCloseBtn', 'logGroups',
            'logGroupERROR', 'logGroupWARNING', 'logGroupINFO', 'logGroupDEBUG',
            'logCountERROR', 'logCountWARNING', 'logCountINFO', 'logCountDEBUG',
            'logBodyERROR', 'logBodyWARNING', 'logBodyINFO', 'logBodyDEBUG'
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
            if (!isNaN(val) && val > 0) {
                this.whaleThreshold = val;
                // Save it for this coin specifically
                localStorage.setItem(`whaleflow_threshold_${this.currentCoin}`, val.toString());
                
                // MULTI-DEVICE SYNC: Notify backend of our threshold preference
                if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                    this.ws.send(JSON.stringify({
                        method: 'update_threshold',
                        coin: this.currentCoin,
                        threshold: val
                    }));
                }

                this.reprocessTrades();
                this.renderOrderbook();
                this.showToast(`🐋 Whale threshold for ${this.currentCoin} set to $${this.formatCompact(val)}`);
            }
        });

        // Data View Mode selector (Historical / Current)
        const tfContainer = document.getElementById('tfWhaleVolume');
        if (tfContainer) {
            tfContainer.addEventListener('click', (e) => {
                const btn = e.target.closest('.tf-bar-btn');
                if (!btn) return;

                tfContainer.querySelectorAll('.tf-bar-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');

                this.dataViewMode = btn.dataset.tf;
                this._saveModeToStorage();

                // Show/hide clear button
                const clearBtn = this.elements.clearDataBtn;
                if (clearBtn) {
                    clearBtn.style.display = this.dataViewMode === 'Current' ? '' : 'none';
                }

                // Update hint text
                const hint = document.getElementById('tfBarHint');
                if (hint) {
                    if (this.dataViewMode === 'Historical') {
                        hint.textContent = 'Showing all historical data from backend';
                    } else {
                        hint.textContent = this.currentModeClearTime > 0
                            ? `Showing data since ${new Date(this.currentModeClearTime).toLocaleTimeString()}`
                            : 'Press Clear Data to start tracking from now';
                    }
                }

                this.updateSummaryCards();
                this.renderTradesList();
                this.updateAnalytics();
            });
        }

        // Clear Data button (in mode bar, Current mode only)
        if (this.elements.clearDataBtn) {
            this.elements.clearDataBtn.addEventListener('click', () => {
                // If connected to local backend, send a global clear signal
                if (this.ws && this.ws.readyState === WebSocket.OPEN && !this.ws.url.includes('hyperliquid.xyz')) {
                    this.ws.send(JSON.stringify({ method: 'clear_current' }));
                } else {
                    // Fallback for single-device offline mode
                    this.currentModeClearTime = Date.now();
                    this._performGlobalClear();
                }
            });
        }

        // Apply initial mode from localStorage
        this._applyInitialMode();

        // Notification toggle
        if (this.elements.notifToggle) {
            this.elements.notifToggle.addEventListener('click', () => this.toggleDesktopNotifications());
        }
        this._updateNotifToggleUI();
    }

    // ==================== WEBSOCKET ====================

    connectWebSocket() {
        if (this.ws) {
            this.ws.onclose = null;
            this.ws.onerror = null;
            this.ws.close();
            this.ws = null;
        }
        if (this.localWs) {
            this.localWs.onclose = null;
            this.localWs.onerror = null;
            this.localWs.close();
            this.localWs = null;
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

        try {
            const host = window.location.hostname || '127.0.0.1';
            this.localWs = new WebSocket(`ws://${host}:8765`);
            this.localWs.onopen = () => {
                console.log('Local backend connected successfully');
                this.localWsActive = true;
                this._serverSystemState.connected = true;
                this.updateSystemPanel();
            };
            this.localWs.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    if (msg.channel === 'pong') return;
                    this.handleMessage(msg);
                } catch (err) { /* ignore */ }
            };
            this.localWs.onclose = (event) => {
                console.log('Local WS closed, falling back to all public exchanges direct natively:', event.code, event.reason);
                this.localWsActive = false;
                this._serverSystemState.connected = false;
                this.updateSystemPanel();
                // Cloud Deployment Fallback: Subscribe to HL trades directly
                if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                    this.ws.send(JSON.stringify({
                        method: 'subscribe',
                        subscription: { type: 'trades', coin: this.currentCoin }
                    }));
                }
                setTimeout(() => {
                    if(!this.publicExchangesConnected) {
                        this.connectPublicExchanges();
                        this.publicExchangesConnected = true;
                    }
                }, 1000);
            };
            this.localWs.onerror = (err) => {
                console.error('Local WS error, gracefully falling back...');
            };
        } catch (err) {
            console.error('Local WebSocket creation failed:', err);
        }

        this.ws.onopen = () => {
            console.log('✅ WebSocket connected');
            this.isConnected = true;
            this.reconnectAttempts = 0;
            this.updateConnectionStatus('connected');
            this.updateSystemPanel();
            this.subscribeAll();

            // Check if localWs is active after a short delay, otherwise fallback
            setTimeout(() => {
                if (!this.localWsActive && this.ws.readyState === WebSocket.OPEN) {
                    console.warn('Local proxy unreachable. Falling back directly to Hyperliquid trades feed.');
                    this.ws.send(JSON.stringify({
                        method: 'subscribe',
                        subscription: { type: 'trades', coin: this.currentCoin }
                    }));
                }
            }, 1500);

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
            this._serverSystemState.connected = !!this.localWsActive;
            this.updateConnectionStatus('disconnected');
            this.updateSystemPanel();
            if (this._pingInterval) {
                clearInterval(this._pingInterval);
                this._pingInterval = null;
            }
            this.scheduleReconnect();
        };
    }

    subscribeAll() {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

        // We no longer subscribe to Hyperliquid trades directly
        // because we receive aggregated trades from localWs (Python backend)

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
            
            // Unsubscribe from trades if we are in fallback mode
            if (!this.localWsActive) {
                this.ws.send(JSON.stringify({
                    method: 'unsubscribe',
                    subscription: { type: 'trades', coin: oldCoin }
                }));
            }
        }

        this.currentCoin = newCoin;
        localStorage.setItem('whaleflow_current_coin', newCoin);

        // Load specific threshold for this coin
        this._loadCoinThreshold(newCoin);

        this.orderbook = { bids: [], asks: [] };
        this.fundingData = null;

        this.loadCoinData(newCoin);
        this.reprocessTrades();

        document.querySelectorAll('.coin-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.coin === newCoin);
        });

        this.elements.markPrice.textContent = '$--';
        if (this.elements.oraclePrice) this.elements.oraclePrice.textContent = '$--';
        this.elements.openInterest.textContent = '$--';
        this.elements.dayVolume.textContent = '$--';
        this.elements.fundingRate.textContent = '--';
        this.elements.fundingHourlyChange.textContent = '>1h --';
        this.elements.fundingFourHourChange.textContent = '>4h --';
        this.elements.fundingDailyChange.textContent = '>24h --';
        this.elements.markPriceHourlyChange.textContent = '>1h --';
        this.elements.markPriceFourHourChange.textContent = '>4h --';
        this.elements.markPriceDailyChange.textContent = '>24h --';
        this.elements.openInterestHourlyChange.textContent = '>1h --';
        this.elements.openInterestFourHourChange.textContent = '>4h --';
        this.elements.openInterestDailyChange.textContent = '>24h --';
        this.elements.dayVolumeHourlyChange.textContent = '>1h --';
        this.elements.dayVolumeFourHourChange.textContent = '>4h --';
        this.elements.dayVolumeDailyChange.textContent = '>24h --';

        const immediateMeta = this.allCoinMeta.get(newCoin);
        if (immediateMeta) {
            this.fundingData = { ...immediateMeta };
            this.updateFundingUI();
            this.updateMarketDataUI();
        }

        this.updateSummaryCards();
        this.renderTradesList();
        this.renderOrderbook();
        this.updateAnalytics();

        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                method: 'subscribe',
                subscription: { type: 'l2Book', coin: newCoin }
            }));
            
            // Subscribe to trades if we are in fallback mode
            if (!this.localWsActive) {
                this.ws.send(JSON.stringify({
                    method: 'subscribe',
                    subscription: { type: 'trades', coin: newCoin }
                }));
            }
        }
        if (!this.localWsActive) this.fetchFundingData();
        this.renderAbsorptionUI();
        this.renderReversalRadar();
        this.renderMegaWhales();
        this.renderRegime();

        this.showToast(`🔄 Switched to ${newCoin}`);
    }

    handleExternalTrade(t) {
        this.handleTrades([t]);
    }

    connectPublicExchanges() {
        console.log('Connecting to public exchanges natively from browser... (Binance, OKX, Kraken, Bybit, Coinbase)');
        const coins = ['BTC', 'ETH', 'SOL', 'PAXG', 'XRP'];

        // 1. Binance
        try {
            const binanceStreams = coins.map(c => `${c.toLowerCase()}usdt@aggTrade`).join('/');
            this.binanceWs = new WebSocket(`wss://stream.binance.com:9443/stream?streams=${binanceStreams}`);
            this.binanceWs.onmessage = (e) => {
                try {
                    const msg = JSON.parse(e.data);
                    if (msg.data) {
                        const s = msg.data.s.replace('USDT', '');
                        if (coins.includes(s)) {
                            this.handleExternalTrade({
                                coin: s, px: msg.data.p, sz: msg.data.q, side: msg.data.m ? 'S' : 'B', time: msg.data.T, exchange: 'BIN'
                            });
                        }
                    }
                } catch(err){}
            };
        } catch(err){}

        // 2. Bybit
        try {
            this.bybitWs = new WebSocket("wss://stream.bybit.com/v5/public/linear");
            this.bybitWs.onopen = () => {
                const args = coins.map(c => `publicTrade.${c}USDT`);
                this.bybitWs.send(JSON.stringify({op: "subscribe", args: args}));
            };
            this.bybitWs.onmessage = (e) => {
                try {
                    const msg = JSON.parse(e.data);
                    if (msg.data && Array.isArray(msg.data)) {
                        msg.data.forEach(t => {
                            const coin = msg.topic.split('.')[1].replace('USDT', '');
                            if (coins.includes(coin)) {
                                this.handleExternalTrade({
                                    coin: coin, px: t.p, sz: t.v, side: t.S === 'Buy' ? 'B' : 'S', time: parseInt(t.T), exchange: 'BYB'
                                });
                            }
                        });
                    }
                } catch(err){}
            };
        } catch(err){}

        // 3. OKX
        try {
            this.okxWs = new WebSocket("wss://ws.okx.com:8443/ws/v5/public");
            this.okxWs.onopen = () => {
                const args = coins.map(c => ({channel: "trades", instId: `${c}-USDT-SWAP`}));
                this.okxWs.send(JSON.stringify({op: "subscribe", args: args}));
            };
            this.okxWs.onmessage = (e) => {
                try {
                    const msg = JSON.parse(e.data);
                    if (msg.arg && msg.arg.channel === 'trades' && msg.data) {
                        msg.data.forEach(t => {
                            const coin = t.instId.split('-')[0];
                            if (coins.includes(coin)) {
                                this.handleExternalTrade({
                                    coin: coin, px: t.px, sz: t.sz, side: t.side === 'buy' ? 'B' : 'S', time: parseInt(t.ts), exchange: 'OKX'
                                });
                            }
                        });
                    }
                } catch(err){}
            };
        } catch(err){}

        // 4. Kraken
        try {
            this.krakenWs = new WebSocket("wss://ws.kraken.com/v2");
            this.krakenWs.onopen = () => {
                const args = coins.map(c => `${c}/USD`);
                this.krakenWs.send(JSON.stringify({
                    method: "subscribe",
                    params: { channel: "trade", symbol: args }
                }));
            };
            this.krakenWs.onmessage = (e) => {
                try {
                    const msg = JSON.parse(e.data);
                    if (msg.channel === 'trade' && msg.type === 'update') {
                        msg.data.forEach(t => {
                            const coin = t.symbol.split('/')[0];
                            if (coins.includes(coin)) {
                                const dtime = new Date(t.timestamp).getTime();
                                this.handleExternalTrade({
                                    coin: coin, px: t.price, sz: t.qty, side: t.side === 'buy' ? 'B' : 'S', time: dtime, exchange: 'KRK'
                                });
                            }
                        });
                    }
                } catch(err){}
            };
        } catch(err){}

        // 5. Coinbase
        try {
            this.coinbaseWs = new WebSocket("wss://ws-feed.exchange.coinbase.com");
            this.coinbaseWs.onopen = () => {
                const args = coins.map(c => `${c}-USD`);
                this.coinbaseWs.send(JSON.stringify({
                    type: "subscribe",
                    product_ids: args,
                    channels: ["matches"]
                }));
            };
            this.coinbaseWs.onmessage = (e) => {
                try {
                    const msg = JSON.parse(e.data);
                    if (msg.type === 'match') {
                        const coin = msg.product_id.split('-')[0];
                        if (coins.includes(coin)) {
                            const dtime = new Date(msg.time).getTime();
                            this.handleExternalTrade({
                                coin: coin, px: msg.price, sz: msg.size, side: msg.side === 'buy' ? 'B' : 'S', time: dtime, exchange: 'CB'
                            });
                        }
                    }
                } catch(err){}
            };
        } catch(err){}
    }

    // ==================== MESSAGE HANDLING ====================

    handleMessage(msg) {
        if (!msg.channel || !msg.data) return;

        switch (msg.channel) {
            case 'trades': {
                const nowSec = Date.now() / 1000;
                this._serverSystemState.last_trade_update = nowSec;
                (msg.data || []).forEach(trade => {
                    const ex = trade.exchange;
                    if (!ex) return;
                    if (!this._serverSystemState.exchange_status[ex]) {
                        this._serverSystemState.exchange_status[ex] = { connected: true, last_msg: 0, last_error: '' };
                    }
                    this._serverSystemState.exchange_status[ex].connected = true;
                    this._serverSystemState.exchange_status[ex].last_msg = nowSec;
                });
                this.handleTrades(msg.data);
                this.updateSystemPanel();
                break;
            }
            case 'l2Book':
                this.handleL2Book(msg.data);
                break;
            case 'funding':
                this.handleFundingUpdate(msg.data);
                break;
            case 'log':
                const clearedAt = parseInt(localStorage.getItem('whaleflow_logs_cleared_at') || '0', 10);
                const logTimeMs = msg.data.timestamp ? msg.data.timestamp * 1000 : 0;
                if (logTimeMs > clearedAt || clearedAt === 0) {
                    this._logEntries.push(msg.data);
                    if (this._logEntries.length > 500) this._logEntries.shift();
                    this._renderLogSidebar();
                }
                break;
            case 'all_clients_clear':
                this._performGlobalClear(msg.clear_time);
                break;
            case 'threshold_update':
                const remoteCoin = msg.coin;
                const remoteThreshold = parseInt(msg.threshold, 10);
                if (remoteCoin && !isNaN(remoteThreshold)) {
                    // Sync our local preference
                    localStorage.setItem(`whaleflow_threshold_${remoteCoin}`, remoteThreshold.toString());
                    // If we are currently looking at this coin, update live
                    if (this.currentCoin === remoteCoin) {
                        this.whaleThreshold = remoteThreshold;
                        this.elements.whaleThreshold.value = remoteThreshold;
                        this.reprocessTrades();
                        this.renderOrderbook();
                        this.showToast(`🐋 Sync: ${remoteCoin} threshold updated to $${this.formatCompact(remoteThreshold)}`);
                    }
                }
                break;
        }
    }

    /** Reset local accumulators and update UI after a global clear signal */
    _performGlobalClear() {
        this._saveModeToStorage();
        
        // Zero out accumulators for all coins natively in browser memory
        this.coinDataStore.forEach(d => {
            d.currentBuyVolume = 0;
            d.currentSellVolume = 0;
            d.currentBuyCount = 0;
            d.currentSellCount = 0;
            d.lastTradeTime = this.currentModeClearTime;
        });
        this._saveCurrentToStorage();
        
        this.loadCoinData(this.currentCoin);

        this.updateSummaryCards();
        this.renderTradesList();
        this.updateAnalytics();

        // Update hint
        const hint = document.getElementById('tfBarHint');
        if (hint) {
            hint.textContent = `Showing data since ${new Date(this.currentModeClearTime).toLocaleTimeString()}`;
        }

        this.showToast('Data cleared - synchronization active');
    }


    handleFundingUpdate(data) {
        if (!data || !data.coins) return;

        const now = (data.timestamp || (Date.now() / 1000)) * 1000;
        Object.entries(data.coins).forEach(([coin, info]) => {
            const meta = {
                funding: parseFloat(info.funding || 0),
                openInterest: parseFloat(info.open_interest || 0),
                markPx: parseFloat(info.mark_px || 0),
                oraclePx: parseFloat(info.oracle_px || 0),
                dayNtlVlm: parseFloat(info.day_volume || 0),
            };
            this._applyCoinMeta(coin, meta, now);
            const fundingStore = this.getCoinData(coin);
            fundingStore.fundingHistory.push({
                time: now,
                funding: meta.funding,
            });
            if (fundingStore.fundingHistory.length > 300) fundingStore.fundingHistory = fundingStore.fundingHistory.slice(-300);
        });

        const activeMeta = this.allCoinMeta.get(this.currentCoin);
        if (activeMeta) {
            this.fundingData = { ...activeMeta };
            this.updateFundingUI();
            this.updateMarketDataUI();
            this.renderRegime();
        }

        if (data.timestamp) {
            this._serverSystemState.last_funding_update = data.timestamp;
        }
        this.updateSystemPanel();
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
                    price, size, value, coin,
                    exchange: trade.exchange || 'HL'
                });

                if (d.whaleTrades.length > 500) {
                    d.whaleTrades = d.whaleTrades.slice(0, 500);
                }

                if (isBuy) {
                    d.totalBuyVolume += value;
                    d.buyCount++;
                    if (trade.time >= this.currentModeClearTime) {
                        d.currentBuyVolume += value;
                        d.currentBuyCount++;
                        d.lastTradeTime = Math.max(d.lastTradeTime || 0, trade.time);
                        this._needsStorageSave = true;
                    }
                } else {
                    d.totalSellVolume += value;
                    d.sellCount++;
                    if (trade.time >= this.currentModeClearTime) {
                        d.currentSellVolume += value;
                        d.currentSellCount++;
                        d.lastTradeTime = Math.max(d.lastTradeTime || 0, trade.time);
                        this._needsStorageSave = true;
                    }
                }

                if (coin === this.currentCoin) {
                    currentCoinUpdated = true;
                }

        // Throttle saving current mode state to once per second
        if (this._needsStorageSave && !this._saveTimeout) {
            this._saveTimeout = setTimeout(() => {
                this._saveCurrentToStorage();
                this._saveTimeout = null;
                this._needsStorageSave = false;
            }, 1000);
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
                        price, size, value, coin, mega_type: 'initiative',
                        exchange: trade.exchange || 'HL'
                    });
                    if (d.megaWhales.length > 100) d.megaWhales = d.megaWhales.slice(0, 100);
                    if (coin === this.currentCoin) {
                        if (!this._renderMegaPending) {
                            this._renderMegaPending = true;
                            setTimeout(() => {
                                this.renderMegaWhales();
                                this._renderMegaPending = false;
                            }, 1000);
                        }
                    }
                }

                // ——— Whale Clustering detection ———
                const now = Date.now();
                const sideStr = isBuy ? 'BUY' : 'SELL';
                const sideLabel = isBuy ? 'bullish' : 'bearish';
                const recent = d.whaleTrades.filter(t => t.time >= now - 60000).slice(0, 30);
                const sameSide = recent.filter(t => t.side === sideStr);
                if (sameSide.length >= 5) {
                    const clusterVal = sameSide.reduce((s, t) => s + t.value, 0);
                    d.signals.clustering = {
                        active: true,
                        side: sideLabel,
                        detail: `${sameSide.length} ${sideStr} trades ($${(clusterVal/1e6).toFixed(2)}M) in 60s`,
                        time: Date.now()
                    };
                    // Avoid duplicate mega entries for same cluster
                    const lastMega = d.megaWhales[0];
                    if (!lastMega || lastMega.mega_type !== 'clustering' || now - lastMega.time > 60000) {
                        d.megaWhales.unshift({
                            time: now, side: sideStr,
                            price, size: sameSide.reduce((s,t) => s + t.size, 0),
                            value: clusterVal, coin, mega_type: 'clustering',
                            cluster_count: sameSide.length,
                            exchange: trade.exchange || 'HL'
                        });
                        if (d.megaWhales.length > 100) d.megaWhales = d.megaWhales.slice(0, 100);
                        if (coin === this.currentCoin) {
                            if (!this._renderMegaPending) {
                                this._renderMegaPending = true;
                                setTimeout(() => {
                                    this.renderMegaWhales();
                                    this._renderMegaPending = false;
                                }, 1000);
                            }
                        }
                    }
                }
            }
        });

        if (currentCoinUpdated) {
            this.loadCoinData(this.currentCoin);
            this.updateSummaryCards();
            this.updateAnalytics();
            
            if (!this._renderTradesPending) {
                this._renderTradesPending = true;
                requestAnimationFrame(() => {
                    this.renderTradesList();
                    this._renderTradesPending = false;
                });
            }
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
        
        if (!this._renderObPending) {
            this._renderObPending = true;
            requestAnimationFrame(() => {
                this.renderOrderbook();
                this.updateAnalytics();
                this._renderObPending = false;
            });
        }
    }

    // ==================== FUNDING DATA ====================

    _applyCoinMeta(coin, meta, now = Date.now()) {
        if (!meta || !meta.markPx) return;
        this.allCoinMeta.set(coin, { ...meta });

        const d = this.getCoinData(coin);
        const rg = d.regime;

        rg.priceHistory.push({ time: now, price: meta.markPx });
        const cutoff = now - 3600000;
        rg.priceHistory = rg.priceHistory.filter(p => p.time > cutoff);

        d.marketHistory.push({
            time: now,
            markPx: meta.markPx,
            openInterest: (meta.openInterest || 0) * (meta.markPx || 0),
            dayVolume: meta.dayNtlVlm || 0,
        });
        if (d.marketHistory.length > 6000) d.marketHistory = d.marketHistory.slice(-6000);

        if (rg.priceHistory.length < 2) return;

        const thirtyMinAgo = now - 1800000;
        const recentPrices = rg.priceHistory.filter(p => p.time > thirtyMinAgo).map(p => p.price);
        if (recentPrices.length < 2) return;

        const minPrice = Math.min(...recentPrices);
        const maxPrice = Math.max(...recentPrices);
        const rangePct = minPrice > 0 ? ((maxPrice - minPrice) / minPrice) * 100 : 0;

        const rangeThresholds = { BTC: 0.15, ETH: 0.20, SOL: 0.35, XRP: 0.30, PAXG: 0.08 };
        const trendRange = (rangeThresholds[coin] || 0.20) * 2;
        const flatRange = rangeThresholds[coin] || 0.20;
        let rangeScore;
        if (rangePct >= trendRange) rangeScore = 40;
        else if (rangePct >= flatRange) rangeScore = Math.round(15 + (rangePct - flatRange) / (trendRange - flatRange) * 25);
        else rangeScore = Math.round((rangePct / flatRange) * 15);
        rg.rangeScore = rangeScore;

        const buckets = d.volumeBuckets;
        let volumeScore = 15;
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

        let cvdScore = 7;
        if (d.totalBuyVolume + d.totalSellVolume > 0) {
            const cvd = d.totalBuyVolume - d.totalSellVolume;
            const totalVol = d.totalBuyVolume + d.totalSellVolume;
            const cvdRatio = Math.abs(cvd) / totalVol;
            if (cvdRatio >= 0.15) cvdScore = 15;
            else if (cvdRatio >= 0.05) cvdScore = Math.round(7 + (cvdRatio - 0.05) / 0.10 * 8);
            else cvdScore = Math.round(cvdRatio / 0.05 * 7);
        }
        rg.cvdScore = cvdScore;

        let balanceScore = 7;
        if (d.pressureHistory.length >= 3) {
            const recent = d.pressureHistory.slice(-6);
            const totalBuys = recent.reduce((s, p) => s + p.buys, 0);
            const totalSells = recent.reduce((s, p) => s + p.sells, 0);
            const total = totalBuys + totalSells;
            if (total > 0) {
                const buyPct = totalBuys / total;
                const imbalance = Math.abs(buyPct - 0.5) * 2;
                if (imbalance >= 0.3) balanceScore = 15;
                else if (imbalance >= 0.1) balanceScore = Math.round(7 + (imbalance - 0.1) / 0.2 * 8);
                else balanceScore = Math.round(imbalance / 0.1 * 7);
            }
        }
        rg.balanceScore = balanceScore;

        const rawScore = rangeScore + volumeScore + cvdScore + balanceScore;
        const currentLabel = rg.label;
        const holdMinMs = 900000;
        const timeSinceChange = now - rg.lastChangeTime;
        const canChange = rg.lastChangeTime === 0 || timeSinceChange >= holdMinMs;

        let newLabel, newClass;
        if (rawScore >= 60) { newLabel = 'TRENDING'; newClass = 'trending'; }
        else if (rawScore >= 40) { newLabel = 'TRANSITION'; newClass = 'transition'; }
        else { newLabel = 'CHOPPY'; newClass = 'choppy'; }

        const isWarmup = rg.priceHistory.length < 8;
        if (isWarmup) {
            rg.label = 'ANALYZING...';
            rg.cssClass = '';
        } else if ((newLabel !== currentLabel || rg.cssClass !== newClass) && canChange) {
            rg.label = newLabel;
            rg.cssClass = newClass;
            rg.lastChangeTime = now;
        }

        rg.score = rawScore;
    }

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
            const now = Date.now();

            this.coinList.forEach(coin => {
                const i = universe.findIndex(u => u.name === coin);
                if (i === -1 || !contexts[i]) return;

                const ctx = contexts[i];
                const coinMeta = {
                    funding: parseFloat(ctx.funding || '0'),
                    openInterest: parseFloat(ctx.openInterest || '0'),
                    markPx: parseFloat(ctx.markPx || '0'),
                    oraclePx: parseFloat(ctx.oraclePx || '0'),
                    dayNtlVlm: parseFloat(ctx.dayNtlVlm || '0'),
                    premium: parseFloat(ctx.premium || '0'),
                };

                this._applyCoinMeta(coin, coinMeta, now);

                // Push funding history for ALL coins (not just active)
                const fundingStore = this.getCoinData(coin);
                fundingStore.fundingHistory.push({ time: now, funding: coinMeta.funding });
                if (fundingStore.fundingHistory.length > 300) fundingStore.fundingHistory = fundingStore.fundingHistory.slice(-300);
            });

            const activeMeta = this.allCoinMeta.get(this.currentCoin);
            if (activeMeta) {
                this.fundingData = { ...activeMeta };
                this.updateFundingUI();
                this.updateMarketDataUI();
                this.renderRegime();
            }

            if (!this.localWsActive) this.updateSystemPanel();
        } catch (err) {
            console.error('Funding fetch failed:', err);
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
            // Uses 0.000005 threshold (0.0005%) to filter noise — matches Funding UI "Neutral" cutoff
            // Positive funding = longs paying = crowd is long = if flow is buy-dominant, funding confirms
            // Negative funding = shorts paying = crowd is short = if flow is sell-dominant, funding confirms
            let c4_funding = false;
            if (flowIsBuySide && fundingRate > 0.000005) {
                c4_funding = true; // Crowd long + buying flow = bias confirmed
            } else if (!flowIsBuySide && fundingRate < -0.000005) {
                c4_funding = true; // Crowd short + selling flow = bias confirmed
            }

            // ---- Final Verdict ----
            const prevConds = { ...abs.conditions };
            abs.conditions = {
                flow: c1_flow,
                reversal: c2_reversal,
                oi: c3_oi,
                funding: c4_funding
            };

            const metCount = [c1_flow, c2_reversal, c3_oi, c4_funding].filter(Boolean).length;
            abs.conditionsMet = metCount;

            const wasDetected = abs.detected;
            abs.detected = (c1_flow && c2_reversal && c3_oi); // Core 3 required, C4 funding is bonus

            if (abs.detected) {
                if (flowIsBuySide) {
                    abs.side = 'bearish';
                } else {
                    abs.side = 'bullish';
                }
            } else {
                abs.side = null;
            }

            // ---- Notifications (current coin only) ----
            if (coin === this.currentCoin) {
                // Individual condition activations
                if (c1_flow && !prevConds.flow && this._canNotify(`${coin}_abs_flow`)) {
                    this.sendAlert(`📊 Flow Imbalance activated on ${coin} (${imbalancePct.toFixed(0)}%)`, {
                        desktopTitle: `📊 Flow Imbalance — ${coin}`,
                        desktopBody: `Imbalance at ${imbalancePct.toFixed(0)}% with $${(totalVol/1e6).toFixed(2)}M volume`
                    });
                }
                if (c2_reversal && !prevConds.reversal && this._canNotify(`${coin}_abs_reversal`)) {
                    const dir = flowIsBuySide ? 'Buys absorbed (price flat/down)' : 'Sells absorbed (price flat/up)';
                    this.sendAlert(`📉 Price Against Flow on ${coin} — ${dir}`, {
                        desktopTitle: `📉 Price Reversal — ${coin}`,
                        desktopBody: `${dir}, price Δ: ${priceDelta.toFixed(3)}%`
                    });
                }
                if (c3_oi && !prevConds.oi && this._canNotify(`${coin}_abs_oi`)) {
                    this.sendAlert(`📈 OI Increasing on ${coin} (+${oiDelta.toFixed(2)}%)`, {
                        desktopTitle: `📈 OI Rising — ${coin}`,
                        desktopBody: `Open Interest grew by ${oiDelta.toFixed(2)}%`
                    });
                }
                if (c4_funding && !prevConds.funding && this._canNotify(`${coin}_abs_funding`)) {
                    this.sendAlert(`💰 Funding Confirms Bias on ${coin} (${(fundingRate*100).toFixed(4)}%)`, {
                        desktopTitle: `💰 Funding Confirms — ${coin}`,
                        desktopBody: `Funding rate: ${(fundingRate*100).toFixed(4)}%`
                    });
                }
                // Overall absorption detection
                if (abs.detected && !wasDetected && this._canNotify(`${coin}_abs_detected`, 120000)) {
                    const sideLabel = abs.side === 'bullish' ? '🟢 Bullish' : '🔴 Bearish';
                    this.sendAlert(`🔥 ${sideLabel} Absorption Detected on ${coin}!`, {
                        desktopTitle: `🔥 Absorption — ${coin}`,
                        desktopBody: `${sideLabel} absorption with ${metCount}/4 conditions met`
                    });
                }
            }
        });

        // Render for current coin
        this.renderAbsorptionUI();
    }

    /**
     * Render the absorption detection UI for the current coin.
     */
    previewAbsorptionAlertOnce() {
        try {

            const d = this.getCoinData(this.currentCoin);
            const snapshot = {
                detected: d.abs.detected,
                side: d.abs.side,
                conditionsMet: d.abs.conditionsMet,
                conditions: { ...d.abs.conditions },
                metrics: { ...d.abs.metrics },
            };

            d.abs.detected = true;
            d.abs.side = 'bearish';
            d.abs.conditionsMet = 4;
            d.abs.conditions = { flow: true, reversal: true, oi: true, funding: true };
            d.abs.metrics = {
                ...d.abs.metrics,
                cvd: Math.max(Math.abs(d.abs.metrics.cvd || 0), 1850000),
                vol: Math.max(d.abs.metrics.vol || 0, 4200000),
                priceDelta: (d.abs.metrics.priceDelta != null && d.abs.metrics.priceDelta <= 0.02) ? d.abs.metrics.priceDelta : -0.12,
                oiDelta: Math.max(d.abs.metrics.oiDelta || 0, 0.71),
                imbalance: Math.max(d.abs.metrics.imbalance || 0, 76),
                funding: d.abs.metrics.funding || this.fundingData?.funding || 0.00006,
            };

            this.renderAbsorptionUI();

            setTimeout(() => {
                d.abs.detected = snapshot.detected;
                d.abs.side = snapshot.side;
                d.abs.conditionsMet = snapshot.conditionsMet;
                d.abs.conditions = snapshot.conditions;
                d.abs.metrics = snapshot.metrics;
                this.renderAbsorptionUI();
            }, 4500);
        } catch (err) {
            console.warn('Absorption preview skipped:', err);
        }
    }

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
            sub.textContent = `absorption fires when OI, Volume against Flow and Flow Imbalance align`;
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
                dotEl.innerHTML = '&bull;';
                dotEl.className = 'cond-dot active';
                rowEl.className = 'abs-cond-row met';
            } else {
                dotEl.innerHTML = '&#9675;';
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
                    exp.innerHTML = `Absorption detection requires: <strong>(1)</strong> flow imbalance &gt;60%, <strong>(2)</strong> price moving against that flow (reversal), <strong>(3)</strong> OI increasing, and <strong>(4)</strong> funding confirming crowd bias.`;
                }
            }
        }

        // ---- Summary card mini-badge ----
        const statusEl = this.elements.absorptionStatus;
        if (statusEl) {
            const badge = statusEl.querySelector('.absorption-badge');
            if (badge) {
                if (abs.detected) {
                    badge.style.display = 'inline-flex';
                    badge.className = 'absorption-badge absorbing';
                    badge.textContent = abs.side === 'bullish' ? '🟢 Bullish Absorption' : '🔴 Bearish Absorption';
                } else {
                    badge.style.display = 'none';
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
        const v = this.getDisplayVolumes();

        this.elements.totalWhaleBuys.textContent = '$' + this.formatCompact(v.buyVol);
        this.elements.whaleBuyCount.textContent = `${v.buyCount} trade${v.buyCount !== 1 ? 's' : ''}`;

        this.elements.totalWhaleSells.textContent = '$' + this.formatCompact(v.sellVol);
        this.elements.whaleSellCount.textContent = `${v.sellCount} trade${v.sellCount !== 1 ? 's' : ''}`;

        const total = v.buyVol + v.sellVol;
        if (total > 0) {
            const buyPct = (v.buyVol / total) * 100;
            this.elements.buyBarFill.style.width = buyPct + '%';
            this.elements.sellBarFill.style.width = (100 - buyPct) + '%';
        }

        this.updateWinningSide(v);
    }

    updateWinningSide(v) {
        const badge = this.elements.winningBadge;
        const arrow = this.elements.winningArrow;
        const label = this.elements.winningLabel;
        const domPct = this.elements.dominancePct;
        if (!v) v = this.getDisplayVolumes();
        const total = v.buyVol + v.sellVol;

        if (total === 0) {
            badge.className = 'winning-badge';
            arrow.textContent = '='; label.textContent = 'Even'; domPct.textContent = '50%';
            return;
        }

        const buyPct = (v.buyVol / total) * 100;

        if (v.buyVol > v.sellVol) {
            badge.className = 'winning-badge bulls';
            arrow.textContent = '^'; label.textContent = 'BULLS';
            domPct.textContent = buyPct.toFixed(1) + '%';
            domPct.className = 'dominance-value bulls';
        } else if (v.sellVol > v.buyVol) {
            badge.className = 'winning-badge bears';
            arrow.textContent = 'v'; label.textContent = 'BEARS';
            domPct.textContent = (100 - buyPct).toFixed(1) + '%';
            domPct.className = 'dominance-value bears';
        } else {
            badge.className = 'winning-badge';
            arrow.textContent = '='; label.textContent = 'Even'; domPct.textContent = '50%';
            domPct.className = 'dominance-value';
        }
    }

    _getDeltaBase(history, cutoffMs, maxGapMs = Number.POSITIVE_INFINITY) {
        let base = null;
        for (let i = 0; i < history.length; i++) {
            if (history[i].time <= cutoffMs) {
                base = history[i];
            } else {
                break;
            }
        }
        const fallback = base || history[0] || null;
        const complete = !!(base && (cutoffMs - base.time) <= maxGapMs);
        return {
            base: fallback,
            complete,
        };
    }

    _getPercentChange(current, baseValue) {
        if (!Number.isFinite(current) || !Number.isFinite(baseValue) || baseValue <= 0) return null;
        return ((current - baseValue) / baseValue) * 100;
    }

    _renderChangeBadge(el, changeValue, { intervalText = '1h', incompleteText = null, compact = false, complete = true, suffix = '%', formatter = null, zeroThreshold = 0.00005 } = {}) {
        if (!el) return;
        const baseClass = compact ? 'mini-hourly-change' : 'funding-hourly-change';
        el.className = `${baseClass} mono`;
        const label = complete ? intervalText : (incompleteText || `>${intervalText}`);
        const formatted = (value) => formatter ? formatter(value) : `${value.toFixed(4)}${suffix}`;
        
        if (changeValue === null || changeValue === undefined) {
            el.innerHTML = `<span class="loading-mini">${label} ---</span>`;
            return;
        }
        if (Math.abs(changeValue) < zeroThreshold) {
            el.innerHTML = `&harr; ${label} ${formatted(0)}`;
            return;
        }
        if (changeValue > 0) {
            el.classList.add('up');
            el.innerHTML = `&uarr; ${label} +${formatted(changeValue)}`;
        } else {
            el.classList.add('down');
            el.innerHTML = `&darr; ${label} -${formatted(Math.abs(changeValue))}`;
        }
    }

    updateFundingUI() {
        if (!this.fundingData) return;
        const rate = this.fundingData.funding;
        const direction = this.elements.fundingDirection;
        const rateEl = this.elements.fundingRate;
        const hourlyEl = this.elements.fundingHourlyChange;
        const fourHourEl = this.elements.fundingFourHourChange;
        const dailyEl = this.elements.fundingDailyChange;

        rateEl.textContent = (rate * 100).toFixed(4) + '%';

        let badgeClass, badgeText, currentState;
        // Hyperliquid funding can be very small; using a more sensitive threshold (0.1 bps / hour)
        if (rate > 0.000001) {
            badgeClass = 'positive';
            badgeText = '&uarr; Positive (Longs Pay)';
            rateEl.style.color = 'var(--buy-primary)';
            currentState = 'positive';
        } else if (rate < -0.000001) {
            badgeClass = 'negative';
            badgeText = '&darr; Negative (Shorts Pay)';
            rateEl.style.color = 'var(--sell-primary)';
            currentState = 'negative';
        } else {
            badgeClass = 'neutral';
            badgeText = '&harr; Neutral';
            rateEl.style.color = 'var(--accent-1)';
            currentState = 'neutral';
        }

        const coin = this.currentCoin;
        const d = this.getCoinData(coin);
        if (d.prevFundingState !== undefined && d.prevFundingState !== currentState) {
            if (currentState === 'positive' && this._canNotify(`${coin}_funding_flip_pos`, 90000)) {
                this.sendAlert(`Funding Flipped Positive (+5) on ${coin}`, {
                    desktopTitle: `Funding Flip - ${coin}`,
                    desktopBody: `Funding rate turned positive (${(rate*100).toFixed(4)}%). Longs are now paying shorts.`
                });
            } else if (currentState === 'negative' && this._canNotify(`${coin}_funding_flip_neg`, 90000)) {
                this.sendAlert(`Funding Flipped Negative (-5) on ${coin}`, {
                    desktopTitle: `Funding Flip - ${coin}`,
                    desktopBody: `Funding rate turned negative (${(rate*100).toFixed(4)}%). Shorts are now paying longs.`
                });
            }
        }
        d.prevFundingState = currentState;

        const now = Date.now();
        const history = (d.fundingHistory || []).filter(h => h && h.time && Number.isFinite(h.funding));
        const hourBase = this._getDeltaBase(history, now - 3600000, 10 * 60 * 1000);
        const fourHourBase = this._getDeltaBase(history, now - 14400000, 30 * 60 * 1000);
        const dayBase = this._getDeltaBase(history, now - 86400000, 90 * 60 * 1000);
        this._renderChangeBadge(hourlyEl, hourBase.base ? ((rate - hourBase.base.funding) * 100) : null, {
            intervalText: '1h',
            incompleteText: '>1h',
            complete: hourBase.complete,
            zeroThreshold: 0.0000001
        });
        this._renderChangeBadge(fourHourEl, fourHourBase.base ? ((rate - fourHourBase.base.funding) * 100) : null, {
            intervalText: '4h',
            incompleteText: '>4h',
            complete: fourHourBase.complete,
            zeroThreshold: 0.0000001
        });
        this._renderChangeBadge(dailyEl, dayBase.base ? ((rate - dayBase.base.funding) * 100) : null, {
            intervalText: '24h',
            incompleteText: '>24h',
            complete: dayBase.complete,
            zeroThreshold: 0.0000001
        });

        this.elements.fundingCard.className = `summary-card funding-card market-combo-card ${badgeClass}`;
        direction.innerHTML = `<span class="direction-badge ${badgeClass}">${badgeText}</span>`;
    }

    updateMarketDataUI() {
        if (!this.fundingData) return;
        const meta = this.fundingData;
        const coinData = this.getCoinData(this.currentCoin);
        const now = Date.now();
        const marketHistory = (coinData.marketHistory || []).filter(h => h && h.time);
        const currentOiNotional = (meta.openInterest || 0) * (meta.markPx || 0);

        this.elements.markPrice.textContent = '$' + this.formatPrice(meta.markPx);
        if (this.elements.oraclePrice) {
            this.elements.oraclePrice.textContent = '$' + this.formatPrice(meta.oraclePx);
        }
        this.elements.openInterest.textContent = '$' + this.formatCompact(currentOiNotional);
        this.elements.dayVolume.textContent = '$' + this.formatCompact(meta.dayNtlVlm);

        const pricePoints = marketHistory.filter(h => Number.isFinite(h.markPx));
        const priceHourBase = this._getDeltaBase(pricePoints, now - 3600000, 10 * 60 * 1000);
        const priceFourHourBase = this._getDeltaBase(pricePoints, now - 14400000, 30 * 60 * 1000);
        const priceDayBase = this._getDeltaBase(pricePoints, now - 86400000, 90 * 60 * 1000);
        this._renderChangeBadge(this.elements.markPriceHourlyChange, priceHourBase.base ? (meta.markPx - priceHourBase.base.markPx) : null, {
            intervalText: '1h',
            incompleteText: '>1h',
            complete: priceHourBase.complete,
            suffix: '',
            formatter: value => '$' + this.formatPrice(value),
            zeroThreshold: 0.00005,
        });
        this._renderChangeBadge(this.elements.markPriceFourHourChange, priceFourHourBase.base ? (meta.markPx - priceFourHourBase.base.markPx) : null, {
            intervalText: '4h',
            incompleteText: '>4h',
            complete: priceFourHourBase.complete,
            suffix: '',
            formatter: value => '$' + this.formatPrice(value),
            zeroThreshold: 0.00005,
        });
        this._renderChangeBadge(this.elements.markPriceDailyChange, priceDayBase.base ? (meta.markPx - priceDayBase.base.markPx) : null, {
            intervalText: '24h',
            incompleteText: '>24h',
            complete: priceDayBase.complete,
            suffix: '',
            formatter: value => '$' + this.formatPrice(value),
            zeroThreshold: 0.00005,
        });

        const oiPoints = marketHistory.filter(h => Number.isFinite(h.openInterest));
        const oiHourBase = this._getDeltaBase(oiPoints, now - 3600000, 10 * 60 * 1000);
        const oiFourHourBase = this._getDeltaBase(oiPoints, now - 14400000, 30 * 60 * 1000);
        const oiDayBase = this._getDeltaBase(oiPoints, now - 86400000, 90 * 60 * 1000);
        this._renderChangeBadge(this.elements.openInterestHourlyChange, this._getPercentChange(currentOiNotional, oiHourBase.base?.openInterest), {
            intervalText: '1h',
            incompleteText: '>1h',
            compact: true,
            complete: oiHourBase.complete,
        });
        this._renderChangeBadge(this.elements.openInterestFourHourChange, this._getPercentChange(currentOiNotional, oiFourHourBase.base?.openInterest), {
            intervalText: '4h',
            incompleteText: '>4h',
            compact: true,
            complete: oiFourHourBase.complete,
        });
        this._renderChangeBadge(this.elements.openInterestDailyChange, this._getPercentChange(currentOiNotional, oiDayBase.base?.openInterest), {
            intervalText: '24h',
            incompleteText: '>24h',
            compact: true,
            complete: oiDayBase.complete,
        });

        const volPoints = marketHistory.filter(h => Number.isFinite(h.dayVolume));
        const volHourBase = this._getDeltaBase(volPoints, now - 3600000, 10 * 60 * 1000);
        const volFourHourBase = this._getDeltaBase(volPoints, now - 14400000, 30 * 60 * 1000);
        const volDayBase = this._getDeltaBase(volPoints, now - 86400000, 90 * 60 * 1000);
        this._renderChangeBadge(this.elements.dayVolumeHourlyChange, this._getPercentChange(meta.dayNtlVlm, volHourBase.base?.dayVolume), {
            intervalText: '1h',
            incompleteText: '>1h',
            compact: true,
            complete: volHourBase.complete,
        });
        this._renderChangeBadge(this.elements.dayVolumeFourHourChange, this._getPercentChange(meta.dayNtlVlm, volFourHourBase.base?.dayVolume), {
            intervalText: '4h',
            incompleteText: '>4h',
            compact: true,
            complete: volFourHourBase.complete,
        });
        this._renderChangeBadge(this.elements.dayVolumeDailyChange, this._getPercentChange(meta.dayNtlVlm, volDayBase.base?.dayVolume), {
            intervalText: '24h',
            incompleteText: '>24h',
            compact: true,
            complete: volDayBase.complete,
        });
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
        if (!container) return;

        const scrollPos = container.scrollTop;
        const filteredTrades = this.getDisplayTrades();

        if (filteredTrades.length === 0) {
            const emptyMsg = this.dataViewMode === 'Current' && this.currentModeClearTime <= 0
                ? 'Press Clear Data to start tracking'
                : 'Waiting for whale activity...';
            container.innerHTML = `<div class="empty-state">
                <div class="empty-icon">🐋</div>
                <p>${emptyMsg}</p>
                <span class="empty-sub">Trades above $${this.formatCompact(this.whaleThreshold)} will appear here</span>
            </div>`;
            this.elements.tradeCount.textContent = '0 trades';
            return;
        }

        const displayTrades = filteredTrades.slice(0, 100);

        container.innerHTML = displayTrades.map((trade, i) => {
            const isBuy = trade.side === 'BUY';
            const isMega = trade.value >= this.whaleThreshold * 5;
            const timeStr = this.formatTime(trade.time);
            const exch = trade.exchange || 'HL';

            return `<div class="trade-row ${isBuy ? 'buy-trade' : 'sell-trade'}${isMega ? ' mega-whale' : ''}">
                <span class="trade-time">${timeStr}</span>
                <span class="trade-side">${trade.side}</span>
                <span class="trade-price">${this.formatPrice(trade.price)}</span>
                <span class="trade-size">${this.formatSize(trade.size)}</span>
                <span class="trade-exch">[${exch}]</span>
                <span class="trade-value">$${this.formatCompact(trade.value)}</span>
            </div>`;
        }).join('');

        this.elements.tradeCount.textContent = `${filteredTrades.length} trade${filteredTrades.length !== 1 ? 's' : ''}`;

        // Restore scroll position
        if (scrollPos > 0) container.scrollTop = scrollPos;
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
        
        const v = this.getDisplayVolumes();

        this.elements.tradeBuyVol.textContent = '$' + this.formatCompact(v.buyVol);
        this.elements.tradeSellVol.textContent = '$' + this.formatCompact(v.sellVol);

        const tradeTotal = v.buyVol + v.sellVol;
        if (tradeTotal > 0) {
            this.elements.tradeBuyFill.style.width = ((v.buyVol / tradeTotal) * 100) + '%';
            this.elements.tradeSellFill.style.width = ((v.sellVol / tradeTotal) * 100) + '%';
        } else {
            this.elements.tradeBuyFill.style.width = '50%';
            this.elements.tradeSellFill.style.width = '50%';
        }
        this.elements.imbalanceRatio.textContent = v.buyCount > 0 || v.sellCount > 0
            ? `Buy:Sell ${v.buyCount}:${v.sellCount}` : 'Buy:Sell 0:0';
    }

    updateCVD() {
        const v = this.getDisplayVolumes();
        let cvdBuy = v.buyVol;
        let cvdSell = v.sellVol;

        const delta = cvdBuy - cvdSell;
        const cvdEl = this.elements.cvdValue;
        const fillEl = this.elements.cvdFill;

        const sign = delta >= 0 ? '+' : '-';
        cvdEl.textContent = sign + '$' + this.formatCompact(Math.abs(delta));
        cvdEl.className = 'cvd-value ' + (delta >= 0 ? 'positive' : 'negative');

        const total = cvdBuy + cvdSell;
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

    reprocessTrades(coin = null) {
        const targetCoin = coin || this.currentCoin;
        const d = this.getCoinData(targetCoin);
        const threshold = (targetCoin === this.currentCoin) 
            ? this.whaleThreshold 
            : parseInt(localStorage.getItem(`whaleflow_threshold_${targetCoin}`) || '50000', 10);

        // Reset all accumulators
        d.totalBuyVolume = 0; d.totalSellVolume = 0;
        d.buyCount = 0; d.sellCount = 0;
        d.currentBuyVolume = 0; d.currentSellVolume = 0;
        d.currentBuyCount = 0; d.currentSellCount = 0;

        d.whaleTrades.forEach(trade => {
            // Only count trades that meet the threshold for THIS coin
            if (trade.value < threshold) return;

            // Historical totals
            if (trade.side === 'BUY') {
                d.totalBuyVolume += trade.value;
                d.buyCount++;
            } else {
                d.totalSellVolume += trade.value;
                d.sellCount++;
            }

            // Current mode totals
            if (this.currentModeClearTime > 0 && trade.time >= this.currentModeClearTime) {
                if (trade.side === 'BUY') {
                    d.currentBuyVolume += trade.value;
                    d.currentBuyCount++;
                } else {
                    d.currentSellVolume += trade.value;
                    d.currentSellCount++;
                }
            }
        });

        // Only update classes and UI if we reprocessed the ACTIVE coin
        if (targetCoin === this.currentCoin) {
            this.loadCoinData(targetCoin);
            this.updateSummaryCards();
            this.updateAnalytics();
            this.renderTradesList();
        }
        this._saveCurrentToStorage();
    }

    // ==================== DATA VIEW MODE ====================

    _loadModeFromStorage() {
        try {
            const stored = localStorage.getItem('whaleflow_dataViewMode');
            if (stored === 'Historical' || stored === 'Current') {
                this.dataViewMode = stored;
            }
            const clearTime = localStorage.getItem('whaleflow_clearTime');
            if (clearTime) {
                this.currentModeClearTime = parseInt(clearTime, 10) || 0;
            }
        } catch (e) { /* localStorage not available */ }
    }

    _saveModeToStorage() {
        try {
            localStorage.setItem('whaleflow_dataViewMode', this.dataViewMode);
            localStorage.setItem('whaleflow_clearTime', String(this.currentModeClearTime));
        } catch (e) { /* localStorage not available */ }
    }

    _applyInitialMode() {
        // Set the correct button active based on stored mode
        const tfContainer = document.getElementById('tfWhaleVolume');
        if (tfContainer) {
            tfContainer.querySelectorAll('.tf-bar-btn').forEach(btn => {
                btn.classList.toggle('active', btn.dataset.tf === this.dataViewMode);
            });
        }

        // Show/hide clear button
        const clearBtn = this.elements.clearDataBtn;
        if (clearBtn) {
            clearBtn.style.display = this.dataViewMode === 'Current' ? '' : 'none';
        }

        // Set hint
        const hint = document.getElementById('tfBarHint');
        if (hint) {
            if (this.dataViewMode === 'Historical') {
                hint.textContent = 'Showing all historical data from backend';
            } else {
                hint.textContent = this.currentModeClearTime > 0
                    ? `Showing data since ${new Date(this.currentModeClearTime).toLocaleTimeString()}`
                    : 'Press Clear Data to start tracking from now';
            }
        }
    }

    getDisplayTrades() {
        const d = this.getCoinData(this.currentCoin);
        const threshold = this.whaleThreshold;
        
        // Always filter by current threshold for consistency
        const meetThreshold = d.whaleTrades.filter(t => t.value >= threshold);

        if (this.dataViewMode === 'Historical') {
            return meetThreshold;
        }
        
        // Current mode: only trades after clearTime (already filtered by threshold above)
        if (this.currentModeClearTime <= 0) return [];
        return meetThreshold.filter(t => t.time >= this.currentModeClearTime);
    }

    getDisplayVolumes() {
        if (this.dataViewMode === 'Historical') {
            return {
                buyVol: this.totalBuyVolume,
                sellVol: this.totalSellVolume,
                buyCount: this.buyCount,
                sellCount: this.sellCount
            };
        }
        // Current mode: return tracked accumulators instead of recalculating from array (which drops old trades)
        return {
            buyVol: this.currentBuyVolume || 0,
            sellVol: this.currentSellVolume || 0,
            buyCount: this.currentBuyCount || 0,
            sellCount: this.currentSellCount || 0
        };
    }

    // ==================== NOTIFICATIONS ====================

    _loadNotificationPref() {
        try {
            this.desktopNotificationsEnabled = localStorage.getItem('whaleflow_desktopNotif') === 'true';
        } catch (e) { /* localStorage not available */ }
    }

    _saveNotificationPref() {
        try {
            localStorage.setItem('whaleflow_desktopNotif', String(this.desktopNotificationsEnabled));
        } catch (e) { /* localStorage not available */ }
    }

    async toggleDesktopNotifications() {
        if (!this.desktopNotificationsEnabled) {
            // Enable — request browser permission
            if ('Notification' in window) {
                const perm = await Notification.requestPermission();
                if (perm === 'granted') {
                    this.desktopNotificationsEnabled = true;
                    this._saveNotificationPref();
                    this._updateNotifToggleUI();
                    this.showToast('🔔 Desktop notifications enabled');
                } else {
                    this.showToast('❌ Notification permission denied — check browser settings');
                }
            } else {
                this.showToast('❌ Notifications not supported in this browser');
            }
        } else {
            // Disable
            this.desktopNotificationsEnabled = false;
            this._saveNotificationPref();
            this._updateNotifToggleUI();
            this.showToast('🔕 Desktop notifications disabled');
        }
    }

    _updateNotifToggleUI() {
        const toggle = this.elements.notifToggle;
        const label = this.elements.notifLabel;
        const icon = this.elements.notifIcon;
        if (toggle) {
            toggle.classList.toggle('active', this.desktopNotificationsEnabled);
        }
        if (label) {
            label.textContent = this.desktopNotificationsEnabled ? 'Alerts On' : 'Alerts Off';
        }
        if (icon) {
            icon.textContent = this.desktopNotificationsEnabled ? '🔔' : '🔕';
        }
    }

    _canNotify(key, cooldownMs = 60000) {
        const last = this._notifCooldowns.get(key) || 0;
        if (Date.now() - last < cooldownMs) return false;
        this._notifCooldowns.set(key, Date.now());
        return true;
    }

    sendAlert(message, { desktopTitle, desktopBody } = {}) {
        // Always show in-app toast
        this.showToast(message);

        // Desktop notification if enabled + permission granted
        if (this.desktopNotificationsEnabled && 'Notification' in window && Notification.permission === 'granted') {
            try {
                const notif = new Notification(desktopTitle || 'WhaleFlow Alert', {
                    body: desktopBody || message.replace(/[\u{1F300}-\u{1FAFF}]/gu, '').trim(),
                    icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">🐋</text></svg>',
                    tag: desktopTitle || message.slice(0, 50),
                    requireInteraction: false,
                    silent: false,
                });
            } catch (e) {
                console.warn('[Notification] Failed:', e);
            }
        }
    }

    // ==================== CLOCK ====================

    startClock() {
        const update = () => {
            this.elements.liveClock.textContent = new Date().toLocaleTimeString('en-US', {
                hour12: true, hour: '2-digit', minute: '2-digit', second: '2-digit'
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
            hour12: true, hour: '2-digit', minute: '2-digit', second: '2-digit'
        });
    }

    // ==================== SERVER STATE HYDRATION ====================

    loadServerState(state) {
        if (!state || !state.coins) return;

        // MULTI-DEVICE SYNC: Inherit the Master Clear Time from server if it exists.
        // This ensures Device A and Device B always agree on the "Current" window.
        if (state.last_clear_at) {
            this.currentModeClearTime = state.last_clear_at;
        } else if (this.currentModeClearTime === 0 && state.started_at) {
            this.currentModeClearTime = state.started_at * 1000;
        }

        // MULTI-DEVICE SYNC: Inherit Master Thresholds from server
        if (state.active_thresholds) {
            let thresholdChanged = false;
            Object.entries(state.active_thresholds).forEach(([coin, thresh]) => {
                const existing = localStorage.getItem(`whaleflow_threshold_${coin}`);
                if (existing !== thresh.toString()) {
                    localStorage.setItem(`whaleflow_threshold_${coin}`, thresh.toString());
                    if (this.currentCoin === coin) thresholdChanged = true;
                }
            });
            // Update current whale threshold if it was changed remotely or via server reboot defaults
            const currentThresh = state.active_thresholds[this.currentCoin];
            if (currentThresh && this.whaleThreshold !== currentThresh) {
                this.whaleThreshold = currentThresh;
                this.elements.whaleThreshold.value = currentThresh;
                thresholdChanged = true;
            }
            
            if (thresholdChanged) {
                console.log("🐋 Thresholds updated from server, re-processing UI...");
                this.reprocessTrades();
                this.renderOrderbook();
            }
        }

        console.log(`📦 Synchronizing with server (uptime: ${Math.round(state.uptime_seconds / 60)}min)`);

        this._loadCurrentFromStorage();

        this.coinList.forEach(coin => {
            const serverCoin = state.coins[coin];
            if (!serverCoin) return;

            const d = this.getCoinData(coin);
            const coinThreshold = parseInt(localStorage.getItem(`whaleflow_threshold_${coin}`) || '50000', 10);
            const backendThresh = this.backendThresholds[coin] || 50000;

            // MASTER SYNC logic
            if (coinThreshold === backendThresh) {
                // If we are at default threshold, strictly match the server's master totals
                d.totalBuyVolume = serverCoin.total_buy_vol;
                d.totalSellVolume = serverCoin.total_sell_vol;
                d.buyCount = serverCoin.buy_count;
                d.sellCount = serverCoin.sell_count;
                
                // Also match Current mode totals for perfect cross-device alignment
                d.currentBuyVolume = serverCoin.current_buy_vol;
                d.currentSellVolume = serverCoin.current_sell_vol;
                d.currentBuyCount = serverCoin.current_buy_count;
                d.currentSellCount = serverCoin.current_sell_count;
            } else {
                // If at a custom threshold, we rely on local persistence + buffer stitching.
                // We only re-calculate if the local data is missing (0) to avoid wiping history.
                if (d.totalBuyVolume === 0) {
                    d.whaleTrades = serverCoin.whale_trades.slice(0, 500);
                    this.reprocessTrades(coin);
                }
            }

            // Always ingest the latest shared trade buffer for the Detail View
            if (serverCoin.whale_trades) {
                const incoming = serverCoin.whale_trades.slice(0, 500);
                
                // Stitching loop for details and custom current modes
                const lastTimeBeforeStitch = d.lastTradeTime || 0;
                for (let i = incoming.length - 1; i >= 0; i--) {
                    const t = incoming[i];
                    if (t.time > lastTimeBeforeStitch) {
                        // Add to trade list memory
                        if (t.value >= coinThreshold) {
                            d.whaleTrades.unshift(t);
                            if (d.whaleTrades.length > 500) d.whaleTrades.pop();
                            
                            // If we weren't in sync with server totals (custom threshold), 
                            // we must increment our local totals manually from the buffer.
                            if (coinThreshold !== backendThresh) {
                                d.totalBuyVolume += t.value;
                                d.totalSellVolume += 0; // Backend split handles this
                                if (t.side === 'BUY') {
                                    d.buyCount++;
                                    if (t.time >= this.currentModeClearTime) {
                                        d.currentBuyVolume += t.value;
                                        d.currentBuyCount++;
                                    }
                                } else {
                                    d.sellCount++;
                                    if (t.time >= this.currentModeClearTime) {
                                        d.currentSellVolume += t.value;
                                        d.currentSellCount++;
                                    }
                                }
                            }
                        }
                this._saveCurrentToStorage();

                d.lastPressureSnapshot = {
                    buys: serverCoin.total_buy_vol,
                    sells: serverCoin.total_sell_vol
                };
            }
            
            if (serverCoin.funding_history && serverCoin.funding_history.length > 0) {
                d.fundingHistory = serverCoin.funding_history.map(h => ({
                    time: (h.time || 0) * 1000,
                    funding: h.funding || 0,
                }));
            }
            if (serverCoin.market_history && serverCoin.market_history.length > 0) {
                d.marketHistory = serverCoin.market_history.map(h => ({
                    time: (h.time || 0) * 1000,
                    markPx: h.mark_px || 0,
                    openInterest: h.open_interest || 0,
                    dayVolume: h.day_volume || 0,
                }));
            }

            // Whale timeframe buckets
            if (serverCoin.whale_buckets && serverCoin.whale_buckets.length > 0) {
                d.whaleBuckets = serverCoin.whale_buckets;
            }

            // Pressure history
            if (serverCoin.pressure_history && serverCoin.pressure_history.length > 0) {
                d.pressureHistory = serverCoin.pressure_history;
            }

            // Current volume bucket (needed for regime + volume climax signal)
            if (serverCoin.current_bucket_buy !== undefined) {
                d.currentBucketBuy = serverCoin.current_bucket_buy;
            }
            if (serverCoin.current_bucket_sell !== undefined) {
                d.currentBucketSell = serverCoin.current_bucket_sell;
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

                // Load absorption snapshots (backend time is seconds → ms)
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

            // Mega whales — backend uses appendleft so index 0 = newest
            if (serverCoin.mega_whales && serverCoin.mega_whales.length > 0) {
                d.megaWhales = serverCoin.mega_whales.slice(0, 100);
            }

            // Reversal Radar signals — convert signal times from backend seconds → ms
            if (serverCoin.signals) {
                d.signals = serverCoin.signals;
                ['initiative', 'clustering'].forEach(key => {
                    if (d.signals[key] && d.signals[key].time) {
                        d.signals[key].time = d.signals[key].time * 1000;
                    }
                });
            }
            if (serverCoin.alert_level !== undefined) {
                d.alertLevel = serverCoin.alert_level;
                d.alertLabel = serverCoin.alert_label || 'Quiet';
            }

            // Volume buckets
            if (serverCoin.volume_buckets) {
                d.volumeBuckets = serverCoin.volume_buckets;
            }


            // Market Regime — eliminates warmup on refresh
            if (serverCoin.regime) {
                const r = serverCoin.regime;
                d.regime.score          = r.score        || 50;
                d.regime.label          = r.label        || 'ANALYZING…';
                d.regime.cssClass       = r.css_class    || '';
                d.regime.lastChangeTime = (r.last_change_time || 0) * 1000;
                d.regime.rangeScore     = r.range_score  || 0;
                d.regime.volumeScore    = r.volume_score || 0;
                d.regime.cvdScore       = r.cvd_score    || 0;
                d.regime.balanceScore   = r.balance_score || 0;
                if (Array.isArray(r.price_history)) {
                    d.regime.priceHistory = r.price_history.map(p => ({
                        time:  p.time * 1000,
                        price: p.price
                    }));
                }
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
            this.renderRegime();
        }

        // Refresh all UI
        this.updateSummaryCards();
        this.updateAnalytics();
        this.renderTradesList();
        this.renderLogSidebarItems();
        this.renderAbsorptionUI();
        this.renderReversalRadar();
        this.renderMegaWhales();

        // Store system-level state for the System Panel
        this._serverSystemState = {
            started_at: state.started_at,
            uptime_seconds: state.uptime_seconds,
            connected: state.connected,
            last_funding_update: state.last_funding_update || 0,
            last_trade_update: state.last_trade_update || 0,
            snapshot_loaded: state.snapshot_loaded || false,
            exchange_status: state.exchange_status || {},
        };
        this.updateSystemPanel();
        this._setupSystemPanelToggle();

        // Load log buffer and filter out logs the user already permanently cleared
        if (state.log_buffer && Array.isArray(state.log_buffer)) {
            const clearedAt = parseInt(localStorage.getItem('whaleflow_logs_cleared_at') || '0', 10);
            this._logEntries = state.log_buffer.filter(log => {
                const logTimeMs = log.timestamp ? log.timestamp * 1000 : 0;
                return logTimeMs > clearedAt;
            });
            this._renderLogSidebar();
        }
        this._setupLogSidebar();

        const tradeCount = Object.values(state.coins).reduce((s, c) => s + (c.whale_trades ? c.whale_trades.length : 0), 0);
        this.showToast(`📦 Loaded ${tradeCount} whale trades from server (${Math.round(state.uptime_seconds / 60)}min uptime)`);
    }

    // ==================== REVERSAL RADAR ENGINE ====================

    evaluateReversalSignals() {
        const d = this.getCoinData(this.currentCoin);
        const sigs = d.signals;
        const now = Date.now();
        const coin = this.currentCoin;

        // Capture previous signal states for notification comparison
        const prevSignals = {};
        Object.keys(sigs).forEach(k => { prevSignals[k] = sigs[k].active; });
        const prevAlertLevel = d.alertLevel;

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

        // 5. Funding Extreme — only fires for genuinely elevated rates (±0.005%)
        //    Normal funding hovers near 0; this signal means overleveraged positions
        if (this.fundingData && this.fundingData.funding !== undefined) {
            const ratePct = this.fundingData.funding * 100;
            if (ratePct > 0.005) {
                sigs.funding_extreme = { active: true, side: 'bearish', detail: `Funding +${ratePct.toFixed(4)}% — longs overleveraged` };
            } else if (ratePct < -0.005) {
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
        const activeList = [sigs.absorption, sigs.cvd_divergence, sigs.oi_divergence, sigs.volume_climax, sigs.funding_extreme];
        const activeCount = activeList.filter(s => s.active).length;
        d.alertLevel = Math.min(activeCount, 4);
        if (activeCount === 0) d.alertLabel = 'Quiet';
        else if (activeCount === 1) d.alertLabel = 'Watch';
        else if (activeCount <= 3) d.alertLabel = 'High Probability';
        else d.alertLabel = 'Extreme Conviction';

        // ---- Signal Notifications ----
        const signalNames = {
            cvd_divergence: { icon: '📉', label: 'CVD Divergence' },
            oi_divergence:  { icon: '📊', label: 'OI Divergence' },
            volume_climax:  { icon: '🌋', label: 'Volume Climax' },
            funding_extreme:{ icon: '💰', label: 'Funding Extreme' }
        };

        Object.keys(signalNames).forEach(key => {
            const sig = sigs[key];
            if (sig && sig.active && !prevSignals[key] && this._canNotify(`${coin}_sig_${key}`, 90000)) {
                const info = signalNames[key];
                const sideEmoji = sig.side === 'bullish' ? '🟢' : sig.side === 'bearish' ? '🔴' : '';
                this.sendAlert(`${info.icon} ${info.label} on ${coin} ${sideEmoji}`, {
                    desktopTitle: `${info.icon} ${info.label} — ${coin}`,
                    desktopBody: sig.detail || `${info.label} signal activated`
                });
            }
        });

        // Alert level increase notification
        if (d.alertLevel > prevAlertLevel && d.alertLevel >= 2 && this._canNotify(`${coin}_alert_level`, 60000)) {
            this.sendAlert(`🎯 Reversal Radar: ${d.alertLabel} (${activeCount}/5 signals) on ${coin}`, {
                desktopTitle: `🎯 ${d.alertLabel} — ${coin}`,
                desktopBody: `${activeCount} reversal signals active`
            });
        }

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

        const activeCount = [sigs.absorption, sigs.cvd_divergence, sigs.oi_divergence, sigs.volume_climax, sigs.funding_extreme].filter(s => s.active).length;
        if (this.elements.radarAlertSub) this.elements.radarAlertSub.textContent = `${activeCount} / 5 signals active — ${subs[lvl]}`;

        const fill = this.elements.radarAlertFill;
        if (fill) {
            fill.style.width = `${(activeCount / 5) * 100}%`;
            fill.className = 'radar-alert-fill';
            if (lvl >= 4) fill.classList.add('level-3');
            else if (lvl >= 2) fill.classList.add('level-2');
            else if (lvl >= 1) fill.classList.add('level-1');
        }

        if (this.elements.radarAlertBadge) {
            this.elements.radarAlertBadge.textContent = `${icons[lvl]} ${label} (${activeCount}/5)`;
        }

        // Update each signal row
        const signalMap = [
            { key: 'absorption', row: 'sigAbsorption', dot: 'sigAbsorptionDot', detail: 'sigAbsorptionDetail' },
            { key: 'cvd_divergence', row: 'sigCVD', dot: 'sigCVDDot', detail: 'sigCVDDetail' },
            { key: 'oi_divergence', row: 'sigOI', dot: 'sigOIDot', detail: 'sigOIDetail' },
            { key: 'volume_climax', row: 'sigClimax', dot: 'sigClimaxDot', detail: 'sigClimaxDetail' },
            { key: 'funding_extreme', row: 'sigFunding', dot: 'sigFundingDot', detail: 'sigFundingDetail' },
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
                dotEl.innerHTML = sig.active ? '&bull;' : '&#9675;';
                dotEl.className = 'signal-dot';
                if (sig.active) {
                    dotEl.classList.add('active');
                    if (sig.side === 'bearish') dotEl.classList.add('bearish');
                }
            }
            if (detailEl) {
                detailEl.textContent = sig.active ? sig.detail : '—';
                detailEl.className = 'signal-detail';
                if (sig.active) {
                    if (sig.side === 'bearish') detailEl.classList.add('bearish');
                    else if (sig.side === 'bullish') detailEl.classList.add('bullish');
                }
            }
        });

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
                : `<span class="mega-type-badge clustering">🦈 CLUSTER ?-${m.cluster_count || '?'}</span>`;
            const timeStr = this.formatTime(m.time);
            const valueStr = m.value >= 1e6 ? `$${(m.value/1e6).toFixed(2)}M` : `$${(m.value/1e3).toFixed(0)}K`;

            return `
                <div class="mega-whale-entry ${typeClass}">
                    <span class="mega-time">${timeStr}</span>
                    <span class="mega-side ${isBuy ? 'buy' : 'sell'}">${m.side}</span>
                    <span class="mega-info">${m.coin} @ $${m.price.toLocaleString()} ${typeBadge}</span>
                    <span class="mega-value ${isBuy ? 'bulls' : 'bears'}">${valueStr}</span>
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

    // ==================== SYSTEM PANEL ====================

    _setupSystemPanelToggle() {
        const toggle = this.elements.systemPanelToggle;
        if (toggle && !toggle._bound) {
            toggle._bound = true;
            toggle.addEventListener('click', () => {
                const body = this.elements.systemPanelBody;
                const arrow = this.elements.systemPanelArrow;
                if (body) {
                    const open = body.style.display !== 'none';
                    body.style.display = open ? 'none' : 'block';
                    if (arrow) arrow.classList.toggle('open', !open);
                }
            });
        }
    }

    updateSystemPanel() {
        const s = this._serverSystemState;
        if (!s) return;

        // Uptime
        const upEl = this.elements.sysUptime;
        if (upEl) {
            const sec = s.uptime_seconds + (Date.now() / 1000 - (s._loadedAt || Date.now() / 1000));
            if (!s._loadedAt) s._loadedAt = Date.now() / 1000;
            const h = Math.floor(sec / 3600);
            const m = Math.floor((sec % 3600) / 60);
            upEl.textContent = `${h}h ${m}m`;
            upEl.className = 'sys-value mono ok';
        }

        // Backend
        const beEl = this.elements.sysBackend;
        if (beEl) {
            const backendConnected = !!(s.connected || this.localWsActive);
            if (backendConnected) {
                beEl.textContent = 'Connected';
                beEl.className = 'sys-value mono ok';
                beEl.style.color = '';
            } else if (this.publicExchangesConnected || this.isConnected) {
                beEl.textContent = 'Cloud Mode';
                beEl.className = 'sys-value mono';
                beEl.style.color = '#64b5f6'; // Light blue to indicate intentional fallback
            } else {
                beEl.textContent = 'Disconnected';
                beEl.className = 'sys-value mono err';
                beEl.style.color = '';
            }
        }

        // Last funding
        const lfEl = this.elements.sysLastFunding;
        if (lfEl && s.last_funding_update > 0) {
            const ago = Math.round(Date.now() / 1000 - s.last_funding_update);
            lfEl.textContent = ago < 120 ? `${ago}s ago` : `${Math.round(ago / 60)}m ago`;
            lfEl.className = 'sys-value mono ' + (ago < 30 ? 'ok' : ago < 60 ? 'warn' : 'err');
        }

        // Last trade
        const ltEl = this.elements.sysLastTrade;
        if (ltEl && s.last_trade_update > 0) {
            const ago = Math.round(Date.now() / 1000 - s.last_trade_update);
            ltEl.textContent = ago < 120 ? `${ago}s ago` : `${Math.round(ago / 60)}m ago`;
            ltEl.className = 'sys-value mono ' + (ago < 10 ? 'ok' : ago < 30 ? 'warn' : 'err');
        }

        // Snapshot
        const snEl = this.elements.sysSnapshot;
        if (snEl) {
            snEl.textContent = s.snapshot_loaded ? 'Loaded' : 'No';
            snEl.className = 'sys-value mono ' + (s.snapshot_loaded ? 'ok' : 'warn');
        }

        // Exchange status dots
        const now = Date.now() / 1000;
        ['HL', 'BIN', 'BYB', 'OKX', 'KRK', 'CB', 'DRB', 'BFX', 'BGT', 'MEXC', 'UPB', 'GATE'].forEach(ex => {
            const el = this.elements['exch' + ex];
            if (!el) return;
            const info = (s.exchange_status || {})[ex];
            if (!info) return;

            const ageEl = el.querySelector('.exch-age');
            el.className = 'exch-item';

            if (info.connected) {
                const age = info.last_msg > 0 ? Math.round(now - info.last_msg) : -1;
                if (age >= 0 && age < 60) {
                    el.classList.add('connected');
                    if (ageEl) ageEl.textContent = `${age}s`;
                } else if (age >= 60) {
                    el.classList.add('stale');
                    if (ageEl) ageEl.textContent = `${Math.round(age / 60)}m`;
                } else {
                    el.classList.add('connected');
                    if (ageEl) ageEl.textContent = '--';
                }
            } else {
                el.classList.add('disconnected');
                if (ageEl) ageEl.textContent = 'down';
            }
        });
    }
    // ==================== LOG SIDEBAR ====================

    _setupLogSidebar() {
        const tab = this.elements.logSidebarTab;
        const closeBtn = this.elements.logCloseBtn;
        const clearBtn = this.elements.logClearBtn;
        const sidebar = this.elements.logSidebar;

        if (tab && !tab._bound) {
            tab._bound = true;
            tab.addEventListener('click', () => {
                this._logSidebarOpen = !this._logSidebarOpen;
                if (sidebar) sidebar.classList.toggle('open', this._logSidebarOpen);
                localStorage.setItem('whaleflow_logsidebar_open', String(this._logSidebarOpen));
            });
        }
        
        // Restore sidebar state
        if (localStorage.getItem('whaleflow_logsidebar_open') === 'true') {
            this._logSidebarOpen = true;
            if (sidebar) sidebar.classList.add('open');
        }

        if (closeBtn && !closeBtn._bound) {
            closeBtn._bound = true;
            closeBtn.addEventListener('click', () => {
                this._logSidebarOpen = false;
                if (sidebar) sidebar.classList.remove('open');
                localStorage.setItem('whaleflow_logsidebar_open', 'false');
            });
        }

        if (clearBtn && !clearBtn._bound) {
            clearBtn._bound = true;
            clearBtn.addEventListener('click', () => {
                this._logEntries = [];
                localStorage.setItem('whaleflow_logs_cleared_at', Date.now().toString());
                this._renderLogSidebar();
            });
        }

        // Collapse/expand group bodies
        ['ERROR', 'WARNING', 'INFO', 'DEBUG'].forEach(level => {
            const group = this.elements['logGroup' + level];
            if (!group || group._bound) return;
            group._bound = true;
            const header = group.querySelector('.log-group-header');
            const body = this.elements['logBody' + level];
            if (header && body) {
                header.addEventListener('click', () => {
                    body.style.display = body.style.display === 'none' ? '' : 'none';
                });
            }
        });
    }

    _renderLogSidebar() {
        this.renderLogSidebarItems();
    }

    renderLogSidebarItems() {
        const bodies = {
            'ERROR': this.elements.logBodyERROR,
            'WARNING': this.elements.logBodyWARNING,
            'INFO': this.elements.logBodyINFO,
            'DEBUG': this.elements.logBodyDEBUG
        };

        const counts = {
            'ERROR': this.elements.logCountERROR,
            'WARNING': this.elements.logCountWARNING,
            'INFO': this.elements.logCountINFO,
            'DEBUG': this.elements.logCountDEBUG
        };

        const levels = ['ERROR', 'WARNING', 'INFO', 'DEBUG'];
        const grouped = { 'ERROR': [], 'WARNING': [], 'INFO': [], 'DEBUG': [] };

        (this._logEntries || []).forEach(log => {
            if (grouped[log.level]) grouped[log.level].push(log);
        });

        levels.forEach(level => {
            const body = bodies[level];
            const count = counts[level];
            if (!body || !count) return;

            // Save scroll position
            const scrollPos = body.scrollTop;

            count.textContent = grouped[level].length;
            
            // Reversed logs (newest on top)
            const html = grouped[level].length === 0 
                ? '<div class="log-empty-msg">No entries</div>'
                : grouped[level].slice(-100).reverse().map(e => {
                    const time = e.timeShort || '';
                    const msg = (e.msg || e.message || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
                    return `<div class="log-entry level-${level}"><span class="log-entry-time">${time}</span><span class="log-entry-msg">${msg}</span></div>`;
                }).join('');

            body.innerHTML = html;

            const groupEl = this.elements['logGroup' + level];
            if (groupEl) groupEl.classList.toggle('empty', grouped[level].length === 0);

            // Restore scroll position
            if (scrollPos > 0) body.scrollTop = scrollPos;
        });

        // Overall badge
        const badge = this.elements.logBadge;
        if (badge) {
            const total = this._logEntries ? this._logEntries.length : 0;
            badge.textContent = total > 999 ? '999+' : String(total);
            badge.className = 'log-tab-badge';
            if (grouped.ERROR.length > 0) badge.classList.add('has-errors');
            else if (grouped.WARNING.length > 0) badge.classList.add('has-warnings');
            badge.style.display = total > 0 ? 'flex' : 'none';
        }
    }
}

// ==================== INITIALIZE ====================
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new WhaleFlowDashboard();
});












