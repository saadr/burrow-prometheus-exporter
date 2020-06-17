import yaml
from pathlib import Path

from exporter.burrow_client import BurrowClient
from prometheus_client.core import GaugeMetricFamily

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


     def metric_topic_partition_offset(self, cluster, topic):
         m_partition_offset = GaugeMetricFamily(
             "kafka_topic_partition_offset",
             "The latest partition offset for a topic.",
             labels=["cluster", "topic", "partition"],
         )
         for i, offset in enumerate(self.client.get_topic_partition_offset(cluster, topic)):
             m_partition_offset.add_metric(
                 [cluster, topic, str(i)],
                 offset
             )
         yield m_partition_offset


    def collect(self):
        config = load_config()
        client = BurrowClient(config['burrow']['host'], config['burrow']['port'])
        clusters = client.get_clusters(config['cluster']['include'], config['cluster']['exclude'])

        for cluster in clusters:
            # Topic Metrics
            topics = client.get_topics(clusters, config['topic']['include'], config['topic']['exclude'])
            for topic in topics:
                for metric in config['topic']['metrics']:
                    if metric == ['partition-offset']:
                        yield from self.metric_topic_partition_offset(cluster, topic)


if __name__ == '__main__':
    config = load_config()
    client = BurrowClient(config['burrow']['host'], config['burrow']['port'])
    clusters = client.get_consumers('local', exclude=['burrow-local'])
    print(clusters)
