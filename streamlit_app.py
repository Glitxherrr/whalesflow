import json
import os
import runpy
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
APP_PATH = ROOT_DIR / "apps" / "streamlit" / "app.py"


def run_embedded_app() -> None:
    import streamlit as st

    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))

    os.environ["WHALEFLOW_ENABLE_LOCAL_SERVER"] = "0"

    from collector import HyperliquidCollector

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

    collector = HyperliquidCollector.get_instance()

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

    def build_dashboard_html(state: dict) -> str:
        return html_template.replace(
            "__SERVER_STATE_PLACEHOLDER__",
            json.dumps(state),
        )

    def render_dashboard() -> None:
        st.components.v1.html(
            build_dashboard_html(collector.get_state()),
            height=2200,
            scrolling=True,
        )

    fragment = getattr(st, "fragment", None)
    if callable(fragment):

        @fragment(run_every="30s")
        def auto_refresh_dashboard() -> None:
            render_dashboard()

        auto_refresh_dashboard()
    else:
        render_dashboard()
        st.caption("Auto-refresh is enabled when running on a Streamlit version that supports fragments.")


if APP_PATH.exists():
    runpy.run_path(str(APP_PATH), run_name="__main__")
else:
    run_embedded_app()
