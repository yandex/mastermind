import keys
import os.path
import socket
import threading
import time
import traceback

from cocaine.logging import Logger
import elliptics
import msgpack

import inventory
from config import config
import storage
import timed_queue


logging = Logger()

BASE_PORT = config.get('elliptics_base_port', 1024)
CACHE_DEFAULT_PORT = 9999

BASE_STORAGE_PATH = config.get('elliptics_base_storage_path', '/srv/storage/')
CACHE_DEFAULT_PATH = '/srv/cache/'

RSYNC_MODULE = config.get('restore', {}).get('rsync_use_module') and \
               config['restore'].get('rsync_module')
RSYNC_USER = config.get('restore', {}).get('rsync_user', 'rsync')

logging.info('Rsync module using: %s' % RSYNC_MODULE)
logging.info('Rsync user: %s' % RSYNC_USER)


class Infrastructure(object):

    TASK_SYNC = 'infrastructure_sync'
    TASK_UPDATE = 'infrastructure_update'

    TASK_DC_CACHE_SYNC = 'infrastructure_dc_cache_sync'

    RSYNC_CMD = ('rsync -rlHpogDt --progress '
                 '"{user}@{src_host}:{src_path}data*" "{dst_path}"')
    RSYNC_MODULE_CMD = ('rsync -av --progress '
                        '"rsync://{user}@{src_host}/{module}/{src_path}data*" '
                        '"{dst_path}"')

    def __init__(self):

        # actual init happens in 'init' method
        # TODO: return node back to constructor after wrapping
        #       all the code in a 'mastermind' package
        self.node = None
        self.meta_session = None

        self.state = {}
        self.__state_lock = threading.Lock()
        self.sync_ts = None
        self.state_valid_time = config.get('infrastructure_state_valid_time',
                                           120)
        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

    def init(self, node):
        self.node = node
        self.meta_session = self.node.meta_session

        self._sync_state()

        self.dc_cache = DcCacheItem(self.meta_session,
            keys.MM_DC_CACHE_IDX, keys.MM_DC_CACHE_HOST, self.__tq)
        self.dc_cache._sync_cache()

        self.hostname_cache = HostnameCacheItem(self.meta_session,
            keys.MM_HOSTNAME_CACHE_IDX, keys.MM_HOSTNAME_CACHE_HOST, self.__tq)
        self.hostname_cache._sync_cache()

        self.__tq.add_task_in(self.TASK_UPDATE,
            config.get('infrastructure_update_period', 300),
            self._update_state)

    def get_group_history(self, group_id):
        history = []
        for node_set in self.state[group_id]['nodes']:
            history.append({'set': [node + (port_to_path(node[1]),)
                                    for node in node_set['set']],
                            'timestamp': node_set['timestamp'],
                            'manual': node_set.get('manual', False)})
        return history

    def _sync_state(self):
        try:
            logging.info('Syncing infrastructure state')
            group_ids = set()
            with self.__state_lock:

                idxs = self.meta_session.find_all_indexes([keys.MM_GROUPS_IDX])
                for idx in idxs:
                    data = idx.indexes[0].data

                    state_group = self._unserialize(data)
                    logging.debug('Fetched infrastructure item: %s' %
                                  (state_group,))

                    if (self.state.get(state_group['id']) and
                        state_group['id'] in storage.groups):

                        group = storage.groups[state_group['id']]

                        for nodes_state in reversed(state_group['nodes']):
                            if nodes_state['timestamp'] <= self.state[state_group['id']]['nodes'][-1]['timestamp']:
                                break

                            if nodes_state.get('manual', False):
                                nodes_set = set(nodes_state['set'])
                                for node in group.nodes:
                                    if not (node.host.addr, node.port) in nodes_set:
                                        logging.info('Removing {0} from group {1} due to manual group detaching'.format(node, group.group_id))
                                        group.remove_node(node)
                            group.update_status_recursive()

                    self.state[state_group['id']] = state_group
                    group_ids.add(state_group['id'])

                for gid in set(self.state.keys()) - group_ids:
                    logging.info('Group %d is not found in infrastructure state, '
                                 'removing' % gid)
                    del self.state[gid]

            self.sync_ts = time.time()

            logging.info('Finished syncing infrastructure state')
        except Exception as e:
            logging.error('Failed to sync infrastructure state: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.TASK_SYNC,
                config.get('infrastructure_sync_period', 60),
                self._sync_state)

    @staticmethod
    def _serialize(data):
        return msgpack.packb(data)

    @staticmethod
    def _unserialize(data):
        group_state = msgpack.unpackb(data)
        group_state['nodes'] = list(group_state['nodes'])
        return group_state

    @staticmethod
    def _new_group_state(group_id):
        return {
            'id': group_id,
            'nodes': [],
        }

    def _update_state(self):
        groups_to_update = []
        try:
            logging.info('Updating infrastructure state')

            if self.sync_ts is None:
                raise ValueError('Nothing to update')
            state_time = time.time() - self.sync_ts
            if state_time > self.state_valid_time:
                raise ValueError(
                    'State was not updated for %s seconds, '
                    'considered stale' % (time.time() - self.sync_ts))

            for g in storage.groups.keys():

                group_state = self.state.get(g.group_id,
                                             self._new_group_state(g.group_id))

                storage_nodes = tuple((node.host.addr, node.port)
                                      for node in g.nodes)

                if not storage_nodes:
                    logging.debug('Storage nodes list for group %d is empty, '
                                  'skipping' % (g.group_id,))
                    continue

                with self.__state_lock:
                    if not g.group_id in self.state:
                        # add group to state only if checks succeeded
                        self.state[g.group_id] = group_state

                    cur_group_state = (group_state['nodes'] and
                                       group_state['nodes'][-1]
                                       or {'set': []})

                    state_nodes = tuple(nodes
                                        for nodes in cur_group_state['set'])

                    state_nodes_set = set(state_nodes)

                    # extended storage nodes set which includes newly seen nodes,
                    # do not discard lost nodes
                    ext_storage_nodes = (state_nodes + tuple(
                        n for n in storage_nodes if n not in state_nodes_set))

                    logging.debug('Comparing %s and %s' %
                                  (ext_storage_nodes, state_nodes))

                    if set(ext_storage_nodes) != state_nodes_set:
                        logging.info('Group %d info does not match,'
                                     'last state: %s, current state: %s' %
                                     (g.group_id, state_nodes, ext_storage_nodes))
                        self._update_group(g.group_id, ext_storage_nodes)

            logging.info('Finished updating infrastructure state')
        except Exception as e:
            logging.error('Failed to update infrastructure state: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.TASK_UPDATE,
                config.get('infrastructure_update_period', 300),
                self._update_state)

    def _update_group(self, group_id, new_nodes, manual=False):
        group = self.state[group_id]
        new_state = {'set': new_nodes,
                     'timestamp': time.time()}
        if manual:
            new_state['manual'] = True

        group['nodes'].append(new_state)

        eid = elliptics.Id(keys.MM_ISTRUCT_GROUP % group_id)
        logging.info('Updating state for group %s' % group_id)
        self.meta_session.update_indexes(eid, [keys.MM_GROUPS_IDX],
                                              [self._serialize(group)])

    def detach_node(self, group, host, port):
        with self.__state_lock:
            group_state = self.state[group.group_id]
            state_nodes = list(group_state['nodes'][-1]['set'][:])

            new_state_nodes = []

            for i, state_node in enumerate(state_nodes):
                state_host, state_port = state_node
                if state_host == host and state_port == port:
                    logging.debug('Removing node {0}:{1} from '
                        'group {2} history state'.format(host, port, group.group_id))
                    del state_nodes[i]
                    break
            else:
                raise ValueError('Node {0}:{1} not found in '
                    'group {2} history state'.format(host, port, group.group_id))

            self._update_group(group.group_id, state_nodes, manual=True)


    def restore_group_cmd(self, request):
        group_id = int(request[0])
        user = request[1]
        try:
            dest = request[2]
        except IndexError:
            dest = None

        candidates = set()
        warns = []

        try:
            if not group_id in storage.groups:
                raise ValueError('Group %d is not found' % group_id)

            group = storage.groups[group_id]
            if group.couple:
                candidates.add(group.couple)
                for g in group.couple:
                    if g == group:
                        continue
                    if not group in g.couple:
                        warns.append('Group %s is not found in couple of '
                                     'group %s' % (group.group_id, g.group_id))
                    else:
                        candidates.add(g.couple)
            else:
                candidates.update(c for c in storage.couples if group in c)

            if not candidates:
                raise ValueError('Couples containing group %s are not found' %
                                 group_id)

            couple = candidates.pop()
            if len(candidates) > 0:
                warns.append('More than one couple candidate '
                             'for group restoration: %s' % (candidates,))
                warns.append('Selected couple: %s' % (couple,))

            group_candidates = []
            for g in couple:
                if g == group:
                    continue
                if g.status != storage.Status.BAD:
                    warns.append('Cannot use group %s, status: %s '
                                 '(expected %s)' %
                                 (g.group_id, g.status, storage.Status.BAD))
                else:
                    group_candidates.append(g)

            if not group_candidates:
                raise ValueError('No symmetric groups to restore from')

            group_candidates = filter(lambda g: len(g.nodes) == 1, group_candidates)

            if not group_candidates:
                raise ValueError('No symmetric groups with one node found, '
                                 'multiple nodes group restoration is not supported')

            source_group = group_candidates[0]
            source_node = source_group.nodes[0]

            state = self.get_group_history(group.group_id)[-1]['set']
            if len(state) > 1:
                raise ValueError('Restoring group has more than one node, '
                                 'multiple nodes group restoration is not supported')

            addr, port = state[0][:2]

            if (dest and
                (group.nodes[0].host.addr != addr or
                 group.nodes[0].port != port)):
                warns.append('Restoring group history state does not match '
                             'current state, history will be used for '
                             'path construction: history %s:%s, current %s' %
                             (addr, port, group.nodes[0]))

            if len(source_group.nodes) > 1 or len(state) > 1:
                raise ValueError('Do not know how to restore group '
                                 'with more than one node')

            logging.info('Constructing restore cmd for group %s '
                         'from group %s, (%s)' %
                         (group.group_id, source_group, source_node))
            warns.append('Source group %s (%s)' % (source_group, source_node))

            if RSYNC_MODULE:
                cmd = self.RSYNC_MODULE_CMD.format(
                    user=RSYNC_USER,
                    module=RSYNC_MODULE,
                    src_host=source_node.host.addr,
                    src_path=port_to_path(source_node.port).replace(BASE_STORAGE_PATH, ''),
                    dst_path=dest or port_to_path(port))
            else:
                cmd = self.RSYNC_CMD.format(
                    user=user,
                    src_host=source_node.host.addr,
                    src_path=port_to_path(source_node.port),
                    dst_path=dest or port_to_path(port))

        except ValueError as e:
            warns.append(e.message)
            logging.info('Restore cmd for group %s failed, warns: %s' %
                         (group_id, warns))
            return '', '', warns

        logging.info('Restore cmd for group %s on %s, warns: %s, cmd %s' %
                     (group_id, addr, warns, cmd))
        return addr, cmd, warns

    def get_dc_by_host(self, host):
        return self.dc_cache[host]

    def get_hostname_by_addr(self, addr):
        return self.hostname_cache[addr]


class CacheItem(object):

    def __init__(self, meta_session, idx_key, key_key, tq):
        self.meta_session = meta_session
        self.idx_key = idx_key
        self.key_key = key_key
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
        try:
            logging.info(self.logprefix + 'syncing')
            idxs = self.meta_session.find_all_indexes([self.idx_key])
            for idx in idxs:
                data = msgpack.unpackb(idx.indexes[0].data)

                logging.debug(self.logprefix + 'Fetched item: %s' %
                              (data,))

                try:
                    self.cache[data['key']] = data
                except KeyError:
                    pass

            logging.info(self.logprefix + 'Finished syncing')
        except Exception as e:
            logging.error(self.logprefix + 'Failed to sync: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.taskname,
                self.sync_period, self._sync_cache)

    def _update_cache_item(self, key, val):
        eid = elliptics.Id(self.key_key % key)
        cache_item = {'key': key,
                      'val': val,
                      'ts': time.time()}
        logging.info(self.logprefix + 'Updating item for key %s '
                                      'to value %s' % (key, val))
        self.meta_session.update_indexes(eid, [self.idx_key],
                                              [msgpack.packb(cache_item)])
        self.cache[key] = cache_item

    def __getitem__(self, key):
        try:
            cache_item = self.cache[key]
            if cache_item['ts'] + self.key_expire_time < time.time():
                logging.debug(self.logprefix + 'Item for key %s expired' % (key,))
                raise KeyError
            logging.debug(self.logprefix + 'Using item for key %s from cache' % (key,))
            val = cache_item['val']
        except KeyError:
            logging.debug(self.logprefix + 'Fetching value for key %s from source' % (key,))
            try:
                req_start = time.time()
                val = self.get_value(key)
                logging.info(self.logprefix + 'Fetched value for key %s from source: %s' %
                             (key, val))
            except Exception as e:
                req_time = time.time() - req_start
                logging.error(self.logprefix + 'Failed to fetch value for key {0} (time: {1:.5f}s): {2}\n{3}'.format(
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


infrastructure = Infrastructure()


def port_to_path(port):
    assert port >= BASE_PORT
    if port == CACHE_DEFAULT_PORT:
        return CACHE_DEFAULT_PATH
    return os.path.join(BASE_STORAGE_PATH, str(port - BASE_PORT) + '/')
