import json
import os
import runpy
import sys
import time
import threading
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
APP_PATH = ROOT_DIR / "apps" / "streamlit" / "app.py"

# ── Shared state file (all sessions read/write the same file) ──────────────────
STATE_FILE = ROOT_DIR / "runtime" / "shared_state.json"
STATE_LOCK = threading.Lock()
_WRITE_INTERVAL = 15  # seconds between background state saves


def _ensure_runtime_dir():
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


def _save_state(state: dict) -> None:
    """Atomically write collector state to disk so all sessions share it."""
    _ensure_runtime_dir()
    tmp = STATE_FILE.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        tmp.replace(STATE_FILE)
    except Exception:
        pass


def _load_state() -> dict | None:
    """Read last-saved state from disk, return None if missing/corrupt."""
    try:
        if STATE_FILE.exists():
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


# ── Single collector instance shared across ALL Streamlit sessions ─────────────
def _get_shared_collector():
    """
    Cached at the server level by st.cache_resource — created exactly once
    per Streamlit server process, shared by every user session.
    """
    import streamlit as st

    @st.cache_resource
    def _create():
        if str(ROOT_DIR) not in sys.path:
            sys.path.insert(0, str(ROOT_DIR))

        # Disable the embedded FastAPI/WS server — on Streamlit Cloud, only
        # port 8501 is exposed. Exchange data flows via BroadcastChannel.
        os.environ["WHALEFLOW_ENABLE_LOCAL_SERVER"] = "0"
        from collector import HyperliquidCollector

        collector = HyperliquidCollector.get_instance()

        # Background thread: persist state every _WRITE_INTERVAL seconds
        def _persist_loop():
            while True:
                time.sleep(_WRITE_INTERVAL)
                try:
                    state = collector.get_state()
                    _save_state(state)
                except Exception:
                    pass

        t = threading.Thread(target=_persist_loop, daemon=True)
        t.start()

        return collector

    return _create()


def _get_live_state(collector) -> dict:
    """
    Get a trimmed state snapshot for the live data pump.
    Reduces payload size for frequent broadcasting by limiting history lengths.
    The initial full render has complete history; this only needs recent data.
    """
    try:
        state = collector.get_state()
    except Exception:
        return _load_state() or {}

    # Trim heavy arrays to keep broadcast payload under ~200KB
    for coin_data in state.get('coins', {}).values():
        if 'whale_trades' in coin_data:
            coin_data['whale_trades'] = coin_data['whale_trades'][:200]
        if 'funding_history' in coin_data:
            coin_data['funding_history'] = coin_data['funding_history'][-200:]
        if 'market_history' in coin_data:
            coin_data['market_history'] = coin_data['market_history'][-200:]
    return state


def run_embedded_app() -> None:
    import streamlit as st

    st.set_page_config(
        page_title="WhaleFlow Hyperliquid Tracker",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(
        """
<style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp { background: transparent; }
    .block-container { padding: 0 !important; max-width: 100% !important; }
    iframe { border: none !important; }
</style>
""",
        unsafe_allow_html=True,
    )

    # One shared collector for all sessions
    collector = _get_shared_collector()

    css = (ROOT_DIR / "styles.css").read_text(encoding="utf-8")
    js = (ROOT_DIR / "app.js").read_text(encoding="utf-8")
    html_template = (ROOT_DIR / "index.html").read_text(encoding="utf-8")

    html_template = html_template.replace(
        '<link rel="stylesheet" href="styles.css">',
        f"<style>{css}</style>",
    )
    html_template = html_template.replace(
        '<script src="app.js"></script>',
        """<script>
window.__SERVER_STATE__ = __SERVER_STATE_PLACEHOLDER__;

"""
        + js
        + """
</script>""",
    )

    def get_full_state() -> dict:
        """Full state for initial render — includes complete history."""
        try:
            return collector.get_state()
        except Exception:
            return _load_state() or {}

    def build_dashboard_html(state: dict) -> str:
        return html_template.replace(
            "__SERVER_STATE_PLACEHOLDER__",
            json.dumps(state),
        )

    # ── 1. Main dashboard — rendered ONCE, never destroyed ──────────────────
    st.components.v1.html(
        build_dashboard_html(get_full_state()),
        height=2200,
        scrolling=True,
    )

    # ── 2. Live data pump — tiny hidden fragment that broadcasts fresh state ──
    #    Uses BroadcastChannel API to send data to the persistent dashboard
    #    iframe without destroying/recreating it. The fragment only re-renders
    #    this small hidden component every 5 seconds.
    fragment = getattr(st, "fragment", None)
    if callable(fragment):

        @fragment(run_every="5s")
        def live_data_pump() -> None:
            state = _get_live_state(collector)
            state_json = json.dumps(state)
            st.components.v1.html(
                f"""<script>
try {{
    const bc = new BroadcastChannel('whaleflow_live');
    bc.postMessage({state_json});
    bc.close();
}} catch(e) {{
    console.warn('BroadcastChannel failed:', e);
}}
</script>""",
                height=0,
            )

        live_data_pump()


if APP_PATH.exists():
    runpy.run_path(str(APP_PATH), run_name="__main__")
else:
    run_embedded_app()