import yaml
from pathlib import Path

from exporter.burrow_client import BurrowClient

CONFIG_FILE = Path.cwd() / "config.yaml"


def load_config():
    try:
        with open(CONFIG_FILE) as file:
            return yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        return {}


class MetricsCollector(object):

    def __init__(self, config):
        self.config = config
        self.client = BurrowClient(config.host, config.port)

    def collect(self):
        self.client.get_consumer_lag()


if __name__ == '__main__':
    config = load_config()
    client = BurrowClient(config['burrow']['host'], config['burrow']['port'])
    clusters = client.get_consumers('local', exclude=['burrow-local'])
    print(clusters)
