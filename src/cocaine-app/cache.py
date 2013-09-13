# -*- coding: utf-8 -*-
from cocaine.logging import Logger
import elliptics
import timed_queue


logging = Logger()


class CacheManager(object):

    def __init__(self, node, index_prefix):
        self.__node = node
        self.__index_prefix = index_prefix
        self.__namespaces = set()

        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

        self.__index = {}

    def add_namespace(self, namespace):
        self.__namespaces.add(namespace)

    def update_cache_list(self):
        indexes = [self.__index_prefix + ns for ns in self.__namespaces]
        for item in self.__session.find_any_indexes(indexes):
            try:
                # do we need to split items by namespaces?
                item = json.loads(item.indexes[0].data)
                self.__index[item['key']] = item
            except Exception as e:
                logging.info('Failed to load cache item: %s' % e)
