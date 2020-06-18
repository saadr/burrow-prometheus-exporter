import yaml
import logging
import time
from pathlib import Path

from prometheus_client.exposition import start_http_server

from exporter.burrow_client import BurrowClient
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import REGISTRY

log = logging.getLogger(__name__)

CONFIG_FILE = Path.cwd().parent / "config.yaml"


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


def get_status(status):
    possible_status = {'ERR': 0, 'OK': 1, 'WARN': 2, 'NOTFOUND': 3, 'STOP': 4, 'STALL': 6}
    return possible_status.get(status, 99)


def metric_topic_partition_offset(cluster, topic, offsets):
    m_partition_offset = GaugeMetricFamily(
        "kafka_topic_partition_offset",
        "Topic partitions latest offsets",
        labels=["cluster", "topic", "partition"]
    )
    for i, offset in enumerate(offsets):
        m_partition_offset.add_metric(
            [cluster, topic, str(i)],
            offset
        )
    yield m_partition_offset


def metric_consumer_status(cluster, consumer, status):
    m_status = GaugeMetricFamily(
        "kafka_consumer_status",
        "Consumer status",
        labels=["cluster", "consumer_group"]
    )
    m_status.add_metric(
        [cluster, consumer],
        get_status(status)
    )
    yield m_status


def metric_consumer_total_lag(cluster, consumer, total_lag):
    m_total_lag = GaugeMetricFamily(
        "kafka_consumer_total_lag",
        "Consumer total lag",
        labels=["cluster", "consumer_group"]
    )
    m_total_lag.add_metric(
        [cluster, consumer],
        total_lag
    )
    yield m_total_lag


def metric_consumer_max_lag(cluster, consumer, max_lag):
    m_max_lag = GaugeMetricFamily(
        "kafka_consumer_max_lag",
        "Consumer max lag",
        labels=["cluster", "consumer_group", "topic", "partition"]
    )
    m_max_lag.add_metric(
        [cluster, consumer, max_lag['topic'], str(max_lag['partition'])],
        max_lag['current_lag']
    )
    yield m_max_lag


def metric_partition_lag(cluster, consumer, partitions):
    m_partition_lag = GaugeMetricFamily(
        "kafka_consumer_partition_lag",
        "Consumer partition lag",
        labels=["cluster", "consumer_group", "topic", "partition"]
    )
    for partition in partitions:
        m_partition_lag.add_metric(
            [cluster, consumer, partition['topic'], str(partition['partition'])],
            partition['current_lag']
        )
    yield m_partition_lag


def metric_partition_offset(cluster, consumer, partitions):
    m_partition_offset = GaugeMetricFamily(
        "kafka_consumer_partition_offset",
        "Consumer latest partition offset",
        labels=["cluster", "consumer_group", "topic", "partition"]
    )
    for partition in partitions:
        m_partition_offset.add_metric(
            [cluster, consumer, partition['topic'], str(partition['partition'])],
            partition['end']['offset']
        )
    yield m_partition_offset


def metric_partition_status(cluster, consumer, partitions):
    m_partition_status = GaugeMetricFamily(
        "kafka_consumer_partition_status",
        "Consumer partition status",
        labels=["cluster", "consumer_group", "topic", "partition"]
    )
    for partition in partitions:
        m_partition_status.add_metric(
            [cluster, consumer, partition['topic'], str(partition['partition'])],
            get_status(partition['status'])
        )
    yield m_partition_status


def collect_topic_metrics(client, cluster, topic, metrics):
    if 'partition-offset' in metrics:
        yield from metric_topic_partition_offset(cluster, topic, client.get_topic_partition_offset(cluster, topic))


def collect_consumer_metrics(client, cluster, consumer, metrics):
    response = client.get_consumer_lag(cluster, consumer)
    if 'status' in metrics:
        yield from metric_consumer_status(cluster, consumer, response['status'])

    if 'total-lag' in metrics:
        yield from metric_consumer_total_lag(cluster, consumer, response['totallag'])

    if 'max-lag' in metrics:
        yield from metric_consumer_max_lag(cluster, consumer, response['maxlag'])

    if 'partition-lag' in metrics:
        yield from metric_partition_lag(cluster, consumer, response['partitions'])

    if 'partition-offset' in metrics:
        yield from metric_partition_offset(cluster, consumer, response['partitions'])

    if 'partition-status' in metrics:
        yield from metric_partition_status(cluster, consumer, response['partitions'])


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
