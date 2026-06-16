"""
Prometheus metrics server entrypoint.

Exposes the /metrics endpoint on PROMETHEUS_PORT (default 9100)
for scraping by Prometheus. Runs as a background process alongside
the producer and consumer.

The prometheus_client library maintains a global registry of all
Counter, Gauge, and Histogram objects defined in
infrastructure/monitoring/metrics.py. This server exposes that
registry over HTTP in the Prometheus text exposition format.

Run:
    python -m etl.entrypoints.metrics_server
"""

from __future__ import annotations

import logging
import sys

from prometheus_client import start_http_server

from etl.config.settings import get_settings
from etl.observability.structured_logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    setup_logging(level=settings.monitoring.log_level)

    port = settings.monitoring.prometheus_port_producer

    try:
        start_http_server(port)
        logger.info(
            "Prometheus metrics server started",
            extra={"port": port},
        )

        # Block indefinitely. The server runs in a background daemon thread
        # started by start_http_server(). The main thread must stay alive.
        import time
        while True:
            time.sleep(60)

    except OSError as exc:
        logger.critical(
            "Failed to bind metrics server on port %d: %s",
            port,
            exc,
        )
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Metrics server stopped")


if __name__ == "__main__":
    main()
