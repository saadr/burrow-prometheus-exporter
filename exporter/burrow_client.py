import requests
import logging

log = logging.getLogger(__name__)


def request_burrow(url):
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
        response = request_burrow(f'{self.endpoint}')
        clusters = response['clusters']
        if include:
            clusters = list(set(clusters) & set(include))
        if exclude:
            clusters = list(set(clusters) - set(exclude))
        return clusters

    def get_consumers(self, cluster, include=None, exclude=None):
        response = request_burrow(f'{self.endpoint}/{cluster}/consumer')
        consumers = response['consumers']
        if include:
            consumers = list(set(consumers) & set(include))
        if exclude:
            consumers = list(set(consumers) - set(exclude))
        return consumers

    def get_topics(self, cluster, include=None, exclude=None):
        response = request_burrow(f'{self.endpoint}/{cluster}/topic')
        topics = response['topics']
        if include:
            topics = list(set(topics) & set(include))
        if exclude:
            topics = list(set(topics) - set(exclude))
        return topics


    def get_consumer_lag(self, cluster, consumer_group):
        return request_burrow(f'{self.endpoint}/{cluster}/consumer/{consumer_group}/lag')
