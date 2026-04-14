import os
import time

os.environ.setdefault("WHALEFLOW_ENABLE_LOCAL_SERVER", "1")

from collector import HyperliquidCollector


def main() -> None:
    collector = HyperliquidCollector.get_instance()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.shutdown()


if __name__ == "__main__":
    main()
