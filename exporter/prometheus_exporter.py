import yaml
import logging
import time
from pathlib import Path

from prometheus_client.exposition import start_http_server

from exporter.burrow_client import BurrowClient
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import REGISTRY

log = logging.getLogger(__name__)

CONFIG_FILE = Path.cwd() / "config.yaml"


def load_config():
    try:
        with open(CONFIG_FILE) as file:
            return yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        return {}


config = load_config()


def conf(path):
    children = path.split('.')
    i = 0
    x = config
    while i < len(children):
        x = x.get(children[i])
        if x is None:
            return None
        i = i + 1
    return x


def collect_topic_metrics(client, cluster, topic, metrics):
    if 'partition-offset' in metrics:
        m_partition_offset = GaugeMetricFamily(
            "kafka_topic_partition_offset",
            "*****************************",
            labels=["cluster", "topic", "partition"]
        )
        for i, offset in enumerate(client.get_topic_partition_offset(cluster, topic)):
            m_partition_offset.add_metric(
                [cluster, topic, str(i)],
                offset
            )
        yield m_partition_offset


def collect_consumer_metrics(client, cluster, consumer, metrics):
    response = client.get_consumer_lag(cluster, consumer)
    partitions = response['partitions']

    if 'total-lag' in metrics:
        m_total_lag = GaugeMetricFamily(
            "kafka_consumer_total_lag",
            "*****************************",
            labels=["cluster", "consumer_group"]
        )
        m_total_lag.add_metric(
            [cluster, consumer],
            response['totallag']
        )
        yield m_total_lag

    if 'max-lag' in metrics:
        max_lag_parition = response['maxlag']
        m_max_lag = GaugeMetricFamily(
            "kafka_consumer_max_lag",
            "*****************************",
            labels=["cluster", "consumer_group", "topic", "partition"]
        )
        m_max_lag.add_metric(
            [cluster, consumer, max_lag_parition['topic'], str(max_lag_parition['partition'])],
            max_lag_parition['current_lag']
        )
        yield m_max_lag

    if 'partition-lag' in metrics:
        m_partition_lag = GaugeMetricFamily(
            "kafka_consumer_partition_lag",
            "The latest partition offset for a topic.",
            labels=["cluster", "consumer_group", "topic", "partition"]
        )
        for partition in partitions:
            m_partition_lag.add_metric(
                [cluster, consumer, partition['topic'], str(partition['partition'])],
                partition['current_lag']
            )
        yield m_partition_lag




class MetricsCollector(object):
    def __init__(self):
        pass

    def collect(self):
        client = BurrowClient(conf('burrow.host'), conf('burrow.port'))
        clusters = client.get_clusters(conf('cluster.include'), conf('cluster.exclude'))

        for cluster in clusters:
            # Topic Metrics
            if conf('topic.metrics'):
                topics = client.get_topics(cluster, conf('topic.include'), conf('topic.exclude'))
                for topic in topics:
                    yield from collect_topic_metrics(client, cluster, topic, conf('topic.metrics'))

            if conf('consumer_group.metrics'):
                # Consumer group metrics
                consumers = client.get_consumers(cluster, conf('consumer_group.include'), conf('consumer_group.exclude'))
                for consumer in consumers:
                    yield from collect_consumer_metrics(client, cluster, consumer, conf('consumer_group.metrics'))


if __name__ == '__main__':
    log.info('Starting server...')
    port = 8001
    start_http_server(port)
    log.info(f'Server started on port {port}')
    REGISTRY.register(MetricsCollector())
    while True:
        time.sleep(1)
