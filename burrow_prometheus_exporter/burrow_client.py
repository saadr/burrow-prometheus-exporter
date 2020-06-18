import requests
import logging
import re

log = logging.getLogger(__name__)


def query_burrow(url):
    try:
        response = requests.get(url).json()
        return response

    except requests.exceptions.HTTPError as e:
        log.error(f'Error occurred while requesting {url}')
        raise e


def filter_items_with_pattern(items, pattern):
    p = pattern.replace('*', '.*')
    r = re.compile(f'^{p}$')
    return list(filter(r.match, items))


def include_item(items, item_to_include):
    if item_to_include == '*':
        return items
    return list(set(items) & set(filter_items_with_pattern(items, item_to_include))) if '*' in item_to_include else [item_to_include]


def exclude_item(items, item_to_exclude):
    if item_to_exclude == '*':
        return []
    return filter_items_with_pattern(items, item_to_exclude) if '*' in item_to_exclude else list(set(items) - {item_to_exclude})


def include_exclude(items, include, exclude):
    output = items
    if include:
        output = []
        include = [include] if isinstance(include, str) else include
        for i in include:
            output.extend(include_item(items, i))

    if exclude:
        exclude = [exclude] if isinstance(exclude, str) else exclude
        for i in exclude:
            output = list(set(output) - set(exclude_item(output, i)))

    return output


class BurrowClient:

    def __init__(self, host, port):
        self.endpoint = f'http://{host}:{port}/v3/kafka'

    def get_clusters(self, include=None, exclude=None):
        if exclude == '*':
            return []
        response = query_burrow(f'{self.endpoint}')
        clusters = response['clusters']
        return include_exclude(clusters, include, exclude)

    def get_consumers(self, cluster, include=None, exclude=None):
        if exclude == '*':
            return []
        response = query_burrow(f'{self.endpoint}/{cluster}/consumer')
        consumers = response['consumers']
        return include_exclude(consumers, include, exclude)

    def get_topics(self, cluster, include=None, exclude=None):
        if exclude == '*':
            return []
        response = query_burrow(f'{self.endpoint}/{cluster}/topic')
        topics = response['topics']
        return include_exclude(topics, include, exclude)

    def get_topic_partition_offset(self, cluster, topic):
        response = query_burrow(f'{self.endpoint}/{cluster}/topic/{topic}')
        return response['offsets']

    def get_consumer_lag(self, cluster, consumer):
        response = query_burrow(f'{self.endpoint}/{cluster}/consumer/{consumer}/lag')
        return response['status']
