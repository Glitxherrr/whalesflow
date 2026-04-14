---
title: WhaleFlow Dashboard
emoji: ??
colorFrom: blue
colorTo: indigo
sdk: docker
pinned: false
---

# WhaleFlow Hyperliquid Tracker

A real-time dashboard for tracking mega-whales, absorption, funding flips, and order flow on Hyperliquid.

## Project split

- `apps/huggingface`: FastAPI/WebSocket deployment for Hugging Face Docker Spaces.
- `apps/streamlit`: Streamlit deployment that reuses the same collector logic without starting the embedded FastAPI server.
- `collector.py`: Shared backend logic used by both deployments.
- `index.html`, `styles.css`, `app.js`: Shared frontend assets used by both deployments.

## Run targets

Hugging Face / Docker:

```bash
python app_huggingface.py
```

Streamlit:

```bash
streamlit run apps/streamlit/app.py
```

A compatibility wrapper is also kept at `streamlit_app.py` for platforms that expect that filename.


