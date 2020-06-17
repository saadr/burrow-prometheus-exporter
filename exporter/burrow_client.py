import requests
import logging

log = logging.getLogger(__name__)


def query_burrow(url):
    try:
        response = requests.get(url).json()
        return response

    except requests.exceptions.HTTPError as e:
        log.error(f'Error occurred while requesting {url}')
        raise e


class BurrowClient:

    def __init__(self, host, port):
        self.endpoint = f'http://{host}:{port}/v3/kafka'

    def get_clusters(self, include=None, exclude=None):
        if exclude == '*':
            return []
        response = query_burrow(f'{self.endpoint}')
        clusters = response['clusters']
        if include == '*':
            return clusters
        if include:
            return list(set(clusters) & set(include))
        if exclude:
            return list(set(clusters) - set(exclude))
        return clusters

    def get_consumers(self, cluster, include=None, exclude=None):
        if exclude == '*':
            return []
        response = query_burrow(f'{self.endpoint}/{cluster}/consumer')
        consumers = response['consumers']
        if include == '*':
            return consumers
        if include:
            return list(set(consumers) & set(include))
        if exclude:
            return list(set(consumers) - set(exclude))
        return consumers

    def get_topics(self, cluster, include=None, exclude=None):
        if exclude == '*':
            return []
        response = query_burrow(f'{self.endpoint}/{cluster}/topic')
        topics = response['topics']
        if include == '*':
            return topics
        if include:
            return list(set(topics) & set(include))
        if exclude:
            return list(set(topics) - set(exclude))
        return topics

    def get_topic_partition_offset(self, cluster, topic):
        response = query_burrow(f'{self.endpoint}/{cluster}/topic/{topic}')
        offsets = response['offsets']
        return offsets

    def get_consumer_lag(self, cluster, consumer_group):
        return query_burrow(f'{self.endpoint}/{cluster}/consumer/{consumer_group}/lag')
