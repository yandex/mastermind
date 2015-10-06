import logging
import socket
import time

import msgpack

from config import config
from errors import CacheUpstreamError
import indexes
import inventory
import keys
import timed_queue


logger = logging.getLogger('mm.infrastructure')


class InfrastructureCache(object):

    DEFAULT_FAMILY = config['elliptics']['nodes'][0][2]
    FAMILIES_FALLBACK_PRIO = {
        socket.AF_INET6: (socket.AF_INET,),
        socket.AF_INET: (),
    }

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

        self.ip_addresses_cache = IpAddressesCacheItem(
            meta_session=self.meta_session,
            idx_key=keys.MM_IPADDRESSES_CACHE_IDX,
            key_tpl=keys.MM_IPADDRESSES_CACHE_HOST,
            task_queue=self.__tq
        )

        self.hosttree_cache._sync_cache()

    @staticmethod
    def strictable(cache, key, strict):
        try:
            return cache[key]
        except Exception as e:
            if not strict:
                return cache.fallback_value
            raise CacheUpstreamError(str(e))

    def get_dc_by_host(self, host, strict=True):
        return self.strictable(self.dc_cache, host, strict)

    def get_hostname_by_addr(self, addr, strict=True):
        return self.strictable(self.hostname_cache, addr, strict)

    def get_host_tree(self, hostname, strict=True):
        return self.strictable(self.hosttree_cache, hostname, strict)

    def get_ip_address_by_host(self, host, strict=True):
        addresses = self.strictable(
            cache=self.ip_addresses_cache,
            key=host,
            strict=strict
        )

        if addresses.get(self.DEFAULT_FAMILY, None):
            return addresses[self.DEFAULT_FAMILY][0]

        for family in self.FAMILIES_FALLBACK_PRIO.get(self.DEFAULT_FAMILY, []):
            if addresses.get(family, None):
                return addresses[family][0]

        if strict:
            raise ValueError('Host {} cannot be resolved to ip address'.format(host))

        return None


cache = InfrastructureCache()


class InfrastructureCacheManager(object):
    def __init__(self, meta_session):
        self.meta_session = meta_session
        self.__tq = timed_queue.TimedQueue()

        cache.init(self.meta_session, self.__tq)

    def _start_tq(self):
        self.__tq.start()


class CacheItem(object):

    def __init__(self, meta_session, idx_key, key_tpl, task_queue):
        self.meta_session = meta_session.clone()
        self.idx = indexes.SecondaryIndex(idx_key, key_tpl, self.meta_session)
        self.__tq = task_queue
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

            logger.info('{}finished syncing, time: {:.3f}'.format(
                self.logprefix, time.time() - start_ts))
        except Exception as e:
            logger.exception('{}failed to sync, time: {:.3f}'.format(
                             self.logprefix, time.time() - start_ts))
        finally:
            self.__tq.add_task_in(self.taskname,
                self.sync_period, self._sync_cache)

    def _update_cache_item(self, key, val):
        cache_item = {'key': key,
                      'val': val,
                      'ts': time.time()}
        try:
            self.idx[key] = msgpack.packb(cache_item)
            self.cache[key] = cache_item
        except Exception:
            logger.exception(self.logprefix + 'Updating cache for key {} '
                'failed'.format(key))
            pass

    def __getitem__(self, key):
        try:
            cache_item = self.cache[key]
            if cache_item['ts'] + self.key_expire_time < time.time():
                logger.debug(self.logprefix + 'Value for key {} expired'.format(key))
                raise KeyError
            val = cache_item['val']
        except KeyError:
            try:
                req_start = time.time()
                val = self.get_value(key)
                logger.info(self.logprefix + 'Fetched value for key {} '
                    'from upstream: {}'.format(key, val))
            except Exception as e:
                req_time = time.time() - req_start
                logger.exception(self.logprefix + 'Failed to fetch value '
                    'for key {} from upstream (time: {:.5f}s)'.format(
                    key, req_time))
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

        self.fallback_value = 'unknown'

    def get_value(self, key):
        return inventory.get_dc_by_host(key)


class HostnameCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_hostname_cache_sync'
        self.logprefix = 'hostname cache: '
        self.sync_period = config.get('infrastructure_hostname_cache_update_period', 600)
        self.key_expire_time = config.get('infrastructure_hostname_cache_valid_time', 604800)
        super(HostnameCacheItem, self).__init__(*args, **kwargs)

        self.fallback_value = 'unknown'

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


class IpAddressesCacheItem(CacheItem):
    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_ipaddresses_cache_sync'
        self.logprefix = 'ipaddresses cache: '
        self.sync_period = config.get('infrastructure_ipaddresses_cache_update_period', 600)
        self.key_expire_time = config.get('infrastructure_ipaddresses_cache_valid_time', 604800)
        super(IpAddressesCacheItem, self).__init__(*args, **kwargs)

        self.fallback_value = {}

    def get_value(self, key):
        return inventory.get_host_ip_addresses(key)
