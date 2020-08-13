import time
import logging
from prometheus_client import REGISTRY, start_http_server
from burrow_prometheus_exporter.collector import BurrowMetricsCollector
from burrow_prometheus_exporter.config import Config

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__)
config = Config()


def main():
    logging.info('Starting server...')
    port = config.get('expose_port')
    start_http_server(port)
    logging.info(f'Server started on port {port}')
    REGISTRY.register(BurrowMetricsCollector())
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
