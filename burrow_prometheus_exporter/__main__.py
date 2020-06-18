import time

from burrow_prometheus_exporter.collector import BurrowMetricsCollector
from prometheus_client import REGISTRY, start_http_server
import logging

log = logging.getLogger(__name__)


def main():
    log.info('Starting server...')
    port = 8001
    start_http_server(port)
    log.info(f'Server started on port {port}')
    REGISTRY.register(BurrowMetricsCollector())
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
