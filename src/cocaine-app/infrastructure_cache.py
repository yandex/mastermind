import errno
import logging
import os.path
import socket
import tempfile
import time

import msgpack

from errors import CacheUpstreamError
import inventory
from mastermind_core.config import config
import timed_queue


logger = logging.getLogger('mm.infrastructure')


class InfrastructureCache(object):

    DEFAULT_FAMILY = config['elliptics']['nodes'][0][2]
    FAMILIES_FALLBACK_PRIO = {
        socket.AF_INET6: (socket.AF_INET,),
        socket.AF_INET: (),
    }

    def init(self, tq):
        self.__tq = tq

        self.hostname_cache = HostnameCacheItem(self.__tq)
        self.hostname_cache._sync_cache()

        self.hosttree_cache = HostTreeCacheItem(self.__tq)
        self.hosttree_cache._sync_cache()

        self.ip_addresses_cache = IpAddressesCacheItem(self.__tq)
        self.ip_addresses_cache._sync_cache()

    @staticmethod
    def strictable(cache, key, strict):
        try:
            return cache[key]
        except Exception as e:
            if not strict and hasattr(cache, 'fallback_value'):
                return cache.fallback_value
            raise CacheUpstreamError(str(e))

    def get_dc_by_host(self, host, strict=True):
        try:
            tree = self.strictable(self.hosttree_cache, host, strict)
        except CacheUpstreamError:
            if not strict:
                return 'unknonwn'
            raise
        cur_node = tree
        dc_node_type = inventory.get_dc_node_type()
        while cur_node:
            if cur_node['type'] == dc_node_type:
                return cur_node['name']
            cur_node = cur_node.get('parent')

        # dc node is not found in host tree
        if not strict:
            return 'unknonwn'
        raise CacheUpstreamError('Host {}: dc cannot be determined'.format(host))

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

        if self.DEFAULT_FAMILY in addresses:
            return addresses[self.DEFAULT_FAMILY][0]

        for family in self.FAMILIES_FALLBACK_PRIO.get(self.DEFAULT_FAMILY, []):
            if family in addresses:
                return addresses[family][0]

        if strict:
            raise ValueError('Host {} cannot be resolved to ip address'.format(host))

        return None


cache = InfrastructureCache()


class InfrastructureCacheManager(object):
    def __init__(self):
        self.__tq = timed_queue.TimedQueue()

        cache.init(self.__tq)

    def _start_tq(self):
        self.__tq.start()


class CacheItem(object):

    def __init__(self, task_queue):
        self.__tq = task_queue
        self.cache = {}

        for attr in ('taskname',
                     'logprefix',
                     'sync_period',
                     'key_expire_time',
                     'key_retry_time',
                     'path'):

            if getattr(self, attr) is None:
                raise AttributeError(
                    'Set "{}" attribute explicitly in your class instance'.format(attr)
                )

    def get_value(self, key):
        raise NotImplemented('Method "get_value" should be implemented in derived class')

    def unserialize(self):
        try:
            with open(self.path, 'rb') as cache_file:
                for item in msgpack.Unpacker(cache_file):
                    yield item
        except IOError as e:
            if e.errno == errno.ENOENT:
                # consider file system cache empty when file is not found
                raise StopIteration
            raise

    def serialize(self):
        packer = msgpack.Packer()
        for item in self.cache.itervalues():
            yield packer.pack(item)

    def dump(self):
        cache_file_dir = os.path.dirname(self.path)
        temp_file = tempfile.NamedTemporaryFile(
            prefix='cache_tmp',
            dir=cache_file_dir,
            delete=False,
        )

        with temp_file:
            for serialized_item in self.serialize():
                temp_file.write(serialized_item)

        os.rename(temp_file.name, self.path)

    def _sync_cache(self):
        start_ts = time.time()

        stored_keys = set()
        dump_required = False
        logger.info(self.logprefix + 'syncing')

        try:
            for stored_item in self.unserialize():
                stored_key = stored_item['key']
                stored_keys.add(stored_key)

                if stored_key in self.cache:
                    if stored_item['ts'] >= self.cache[stored_key]['ts']:
                        self.cache[stored_key] = stored_item
                    else:
                        dump_required = True
                else:
                    self.cache[stored_key] = stored_item

            if set(self.cache.iterkeys()) - stored_keys:
                # dump when new keys are found
                dump_required = True

            if dump_required:
                logger.info(
                    '{}dumping updated cache to {} time: {:.3f}'.format(
                        self.logprefix,
                        self.path,
                        time.time() - start_ts
                    )
                )
                self.dump()

            logger.info(
                '{}finished syncing, time: {:.3f}'.format(
                    self.logprefix,
                    time.time() - start_ts
                )
            )
        except Exception:
            logger.exception(
                '{}failed to sync, time: {:.3f}'.format(
                    self.logprefix,
                    time.time() - start_ts
                )
            )
        finally:
            self.__tq.add_task_in(
                self.taskname,
                self.sync_period,
                self._sync_cache
            )

    def _update_cache_item(self, key, val):
        cache_item = {
            'key': key,
            'val': val,
            'ts': time.time(),
        }
        self.cache[key] = cache_item

    def _store_failed_cache_item(self, key):
        fake_cache_item = {
            'key': key,
            'ts': time.time(),
            'failed': True,
        }
        self.cache[key] = fake_cache_item

    def __getitem__(self, key):
        try:
            cache_item = self.cache[key]

            if cache_item.get('failed', False):
                if cache_item['ts'] + self.key_retry_time < time.time():
                    logger.debug(
                        '{prefix}Value for key {key}, failed on last fetch attempt, '
                        'retrying '.format(
                            prefix=self.logprefix,
                            key=key
                        )
                    )
                    raise KeyError
                else:
                    raise CacheUpstreamError('{} is unavailable, will be retried later'.format(key))

            if cache_item['ts'] + self.key_expire_time < time.time():
                logger.debug(self.logprefix + 'Value for key {} expired'.format(key))
                raise KeyError
            val = cache_item['val']
        except KeyError:
            try:
                req_start = time.time()
                val = self.get_value(key)
                logger.info(
                     '{prefix}Fetched value for key {key} from upstream: {val}'.format(
                         prefix=self.logprefix,
                         key=key,
                         val=val,
                     )
                )
            except Exception:
                req_time = time.time() - req_start
                logger.exception(
                    '{prefix}Failed to fetch value for key {key} from upstream (time: '
                    '{time:.5f}s)'.format(
                        prefix=self.logprefix,
                        key=key,
                        time=req_time,
                    )
                )
                self._store_failed_cache_item(key)
                raise
            self._update_cache_item(key, val)

        return val


class HostnameCacheItem(CacheItem):

    CACHE_CFG = config.get('inventory_cache', {}).get(
        'hostname',
        {
            'cache_update_period': 600,
            'key_valid_time': 604800,
            'key_retry_time': 600,
            'path': '/var/tmp/mastermind.hostname.cache',
        }
    )

    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_hostname_cache_sync'
        self.logprefix = 'hostname cache: '
        # TODO: remove backward compatibility with
        # 'infrastructure_hostname_cache_update_period' option
        self.sync_period = config.get(
            'infrastructure_hostname_cache_update_period',
            self.CACHE_CFG['cache_update_period']
        )
        # TODO: remove backward compatibility with
        # 'infrastructure_hostname_cache_valid_time' option
        self.key_expire_time = config.get(
            'infrastructure_hostname_cache_valid_time',
            self.CACHE_CFG['key_valid_time']
        )
        self.key_retry_time = self.CACHE_CFG['key_retry_time']
        self.path = self.CACHE_CFG['path']
        super(HostnameCacheItem, self).__init__(*args, **kwargs)

        self.fallback_value = 'unknown'

    def get_value(self, key):
        return socket.gethostbyaddr(key)[0]


class HostTreeCacheItem(CacheItem):

    CACHE_CFG = config.get('inventory_cache', {}).get(
        'hosttree',
        {
            'cache_update_period': 600,
            'key_valid_time': 604800,
            'key_retry_time': 600,
            'path': '/var/tmp/mastermind.hosttree.cache',
        }
    )

    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_hosttree_cache_sync'
        self.logprefix = 'hosttree cache: '
        # TODO: remove backward compatibility with
        # 'infrastructure_hosttree_cache_update_period' option
        self.sync_period = config.get(
            'infrastructure_hosttree_cache_update_period',
            self.CACHE_CFG['cache_update_period']
        )
        # TODO: remove backward compatibility with
        # 'infrastructure_hosttree_cache_valid_time' option
        self.key_expire_time = config.get(
            'infrastructure_hosttree_cache_valid_time',
            self.CACHE_CFG['key_valid_time']
        )
        self.key_retry_time = self.CACHE_CFG['key_retry_time']
        self.path = self.CACHE_CFG['path']
        super(HostTreeCacheItem, self).__init__(*args, **kwargs)

    def get_value(self, key):
        return inventory.get_host_tree(key)


class IpAddressesCacheItem(CacheItem):

    CACHE_CFG = config.get('inventory_cache', {}).get(
        'ip_addresses',
        {
            'cache_update_period': 600,
            'key_valid_time': 604800,
            'key_retry_time': 600,
            'path': '/var/tmp/mastermind.ip_addresses.cache',
        }
    )

    def __init__(self, *args, **kwargs):
        self.taskname = 'infrastructure_ipaddresses_cache_sync'
        self.logprefix = 'ipaddresses cache: '
        # TODO: remove backward compatibility with
        # 'infrastructure_ipaddresses_cache_update_period' option
        self.sync_period = config.get(
            'infrastructure_ipaddresses_cache_update_period',
            self.CACHE_CFG['cache_update_period']
        )
        # TODO: remove backward compatibility with
        # 'infrastructure_ipaddresses_cache_valid_time' option
        self.key_expire_time = config.get(
            'infrastructure_ipaddresses_cache_valid_time',
            self.CACHE_CFG['key_valid_time']
        )
        self.key_retry_time = self.CACHE_CFG['key_retry_time']
        self.path = self.CACHE_CFG['path']
        super(IpAddressesCacheItem, self).__init__(*args, **kwargs)

        self.fallback_value = {}

    def get_value(self, key):
        return inventory.get_host_ip_addresses(key)
