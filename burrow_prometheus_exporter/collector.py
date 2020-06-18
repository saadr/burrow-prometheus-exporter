import logging
from prometheus_client.core import GaugeMetricFamily

from burrow_prometheus_exporter.burrow_client import BurrowClient
from burrow_prometheus_exporter.config import Config

log = logging.getLogger(__name__)
config = Config()


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


class BurrowMetricsCollector(object):

    def collect(self):
        client = BurrowClient(config.get('burrow.host'), config.get('burrow.port'))
        clusters = client.get_clusters(config.get('cluster.include'), config.get('cluster.exclude'))

        for cluster in clusters:
            # Topic Metrics
            if config.get('topic.metrics'):
                topics = client.get_topics(cluster, config.get('topic.include'), config.get('topic.exclude'))
                for topic in topics:
                    yield from collect_topic_metrics(client, cluster, topic, config.get('topic.metrics'))

            if config.get('consumer_group.metrics'):
                # Consumer group metrics
                consumers = client.get_consumers(cluster, config.get('consumer_group.include'), config.get('consumer_group.exclude'))
                for consumer in consumers:
                    yield from collect_consumer_metrics(client, cluster, consumer, config.get('consumer_group.metrics'))
