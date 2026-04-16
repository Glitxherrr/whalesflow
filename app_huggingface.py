import os
import signal
import time

os.environ.setdefault("WHALEFLOW_ENABLE_LOCAL_SERVER", "1")

from collector import HyperliquidCollector


def main() -> None:
    collector = HyperliquidCollector.get_instance()

    # HuggingFace Spaces sends SIGTERM before killing the container.
    # Catch it so we do a final snapshot save before the process dies,
    # giving the next cold-start the freshest possible state to restore from.
    def _handle_sigterm(signum, frame):
        collector.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.shutdown()


if __name__ == "__main__":
    main()
