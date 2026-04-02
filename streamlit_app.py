"""
WhaleFlow — Streamlit Dashboard
Serves the HTML/CSS/JS dashboard with pre-loaded server state.
The Python collector runs continuously in the background.
When a user opens the page, they get all accumulated data instantly.
"""

import streamlit as st
import json
import os

# Must be the first Streamlit call
st.set_page_config(
    page_title="WhaleFlow — Hyperliquid Whale Tracker",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Hide Streamlit chrome for a clean dashboard look
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp { background: transparent; }
    .block-container { padding: 0 !important; max-width: 100% !important; }
    iframe { border: none !important; }
</style>
""", unsafe_allow_html=True)

# ---- Start the persistent collector (singleton — only starts once) ----
from collector import HyperliquidCollector
collector = HyperliquidCollector.get_instance()

# ---- Get accumulated server state ----
server_state = collector.get_state()

# ---- Load dashboard files ----
base_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(base_dir, 'styles.css'), 'r', encoding='utf-8') as f:
    css = f.read()

with open(os.path.join(base_dir, 'app.js'), 'r', encoding='utf-8') as f:
    js = f.read()

with open(os.path.join(base_dir, 'index.html'), 'r', encoding='utf-8') as f:
    html_template = f.read()

# ---- Build the combined HTML ----
# Remove the external CSS/JS links from the HTML template and inline them
# Also inject the server state so the JS can load it on init

# Strip existing <link stylesheet> and <script src> tags
html_template = html_template.replace(
    '<link rel="stylesheet" href="styles.css">',
    f'<style>{css}</style>'
)
html_template = html_template.replace(
    '<script src="app.js"></script>',
    f"""<script>
// Server-accumulated state — injected by Streamlit backend
window.__SERVER_STATE__ = {json.dumps(server_state)};

{js}
</script>"""
)

# Render the full dashboard
st.components.v1.html(html_template, height=2200, scrolling=True)
