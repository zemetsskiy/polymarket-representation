import logging
import time
from typing import Dict, Any
from ..config import setup_logging
from ..processors import PolymarketSmartMoneyAnalyzer

logger = logging.getLogger(__name__)


class PolymarketSmartMoneyWorker:

    def __init__(self):
        setup_logging()
        logger.info("POLYMARKET SMART MONEY WORKER INITIALIZED")
        self.analyzer = None

    def run(self, limit: int = 10000) -> Dict[str, Any]:
        start_time = time.time()

        try:
            self.analyzer = PolymarketSmartMoneyAnalyzer()
            results = self.analyzer.analyze_smart_money(limit=limit)
            elapsed = time.time() - start_time
            results['elapsed_seconds'] = round(elapsed, 2)
            logger.info(f"Processing time: {elapsed:.2f}s")
            return results
        except Exception as e:
            logger.error(f"Worker failed: {e}", exc_info=True)
            raise
        finally:
            if self.analyzer:
                self.analyzer.close()


def main():
    worker = PolymarketSmartMoneyWorker()
    results = worker.run()
    logger.info(f"Results: {results}")
    return results


if __name__ == '__main__':
    main()
