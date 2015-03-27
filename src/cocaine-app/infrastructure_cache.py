import logging
import socket
import time
import traceback

import msgpack

from config import config
import indexes
import inventory
import keys
import timed_queue


logger = logging.getLogger('mm.infrastructure')


class InfrastructureCache(object):
    def init(self, meta_session, tq):
        self.meta_session = meta_session
        self.__tq = tq

        self.dc_cache = DcCacheItem(self.meta_session,
            keys.MM_DC_CACHE_IDX, keys.MM_DC_CACHE_HOST, self.__tq)
        self.dc_cache._sync_cache()

        self.hostname_cache = HostnameCacheItem(self.meta_session,
            keys.MM_HOSTNAME_CACHE_IDX, keys.MM_HOSTNAME_CACHE_HOST, self.__tq)
        self.hostname_cache._sync_cache()

        self.hosttree_cache = HostTreeCacheItem(self.meta_session,
            keys.MM_HOSTTREE_CACHE_IDX, keys.MM_HOSTTREE_CACHE_HOST, self.__tq)
        self.hosttree_cache._sync_cache()

    def get_dc_by_host(self, host):
        return self.dc_cache[host]

    def get_hostname_by_addr(self, addr):
        return self.hostname_cache[addr]

    def get_host_tree(self, hostname):
        return self.hosttree_cache[hostname]


cache = InfrastructureCache()


class InfrastructureCacheManager(object):
    def __init__(self, meta_session):
        self.meta_session = meta_session
        self.__tq = timed_queue.TimedQueue()

        cache.init(self.meta_session, self.__tq)

    def _start_tq(self):
        self.__tq.start()


class CacheItem(object):

    def __init__(self, meta_session, idx_key, key_key, tq):
        self.meta_session = meta_session.clone()
        self.idx = indexes.SecondaryIndex(idx_key, key_key, self.meta_session)
        self.__tq = tq
        self.cache = {}

        for attr in ['taskname', 'logprefix', 'sync_period', 'key_expire_time']:
            if getattr(self, attr) is None:
                raise AttributeError('Set "{0}" attribute explicitly in your '
                                 'class instance'.format(attr))

    def get_value(self, key):
        raise NotImplemented('Method "get_value" should be implemented in '
                             'derived class')

    def _sync_cache(self):
        start_ts = time.time()
        try:
            logger.info(self.logprefix + 'syncing')
            for data in self.idx:
                data = msgpack.unpackb(data)

                try:
                    self.cache[data['key']] = data
                except KeyError:
                    pass

            logger.info('{0}finished syncing, time: {1:.3f}'.format(
                self.logprefix, time.time() - start_ts))
        except Exception as e:
            logger.error('{0}failed to sync, time: {1:.3f}, {2}\n{3}'.format(
                            self.logprefix, time.time() - start_ts,
                            e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.taskname,
                self.sync_period, self._sync_cache)

    def _update_cache_item(self, key, val):
        cache_item = {'key': key,
                      'val': val,
                      'ts': time.time()}
        logger.info(self.logprefix + 'Updating item for key %s '
                                      'to value %s' % (key, val))
        self.idx[key] = msgpack.packb(cache_item)
        self.cache[key] = cache_item

    def __getitem__(self, key):
        try:
            cache_item = self.cache[key]
            if cache_item['ts'] + self.key_expire_time < time.time():
                logger.debug(self.logprefix + 'Item for key %s expired' % (key,))
                raise KeyError
            val = cache_item['val']
        except KeyError:
            logger.debug(self.logprefix + 'Fetching value for key %s from source' % (key,))
            try:
                req_start = time.time()
                val = self.get_value(key)
                logger.info(self.logprefix + 'Fetched value for key %s from source: %s' %
                             (key, val))
            except Exception as e:
                req_time = time.time() - req_start
                logger.error(self.logprefix + 'Failed to fetch value for key {0} (time: {1:.5f}s): {2}\n{3}'.format(
                    key, req_time, str(e), traceback.format_exc()))
                raise
            self._update_cache_item(key, val)

        return val


class DcCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_dc_cache_sync'
        self.logprefix = 'dc cache: '
        self.sync_period = config.get('infrastructure_dc_cache_update_period', 150)
        self.key_expire_time = config.get('infrastructure_dc_cache_valid_time', 604800)
        super(DcCacheItem, self).__init__(*args, **kwargs)

    def get_value(self, key):
        return inventory.get_dc_by_host(key)


class HostnameCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_hostname_cache_sync'
        self.logprefix = 'hostname cache: '
        self.sync_period = config.get('infrastructure_hostname_cache_update_period', 600)
        self.key_expire_time = config.get('infrastructure_hostname_cache_valid_time', 604800)
        super(HostnameCacheItem, self).__init__(*args, **kwargs)

    def get_value(self, key):
        return socket.gethostbyaddr(key)[0]


class HostTreeCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_hosttree_cache_sync'
        self.logprefix = 'hosttree cache: '
        self.sync_period = config.get('infrastructure_hosttree_cache_update_period', 600)
        self.key_expire_time = config.get('infrastructure_hosttree_cache_valid_time', 604800)
        super(HostTreeCacheItem, self).__init__(*args, **kwargs)

    def get_value(self, key):
        return inventory.get_host_tree(key)
