from copy import deepcopy
import logging
import operator
import re
import threading
import time
import traceback

import msgpack

from config import config
from errors import CacheUpstreamError
import helpers as h
import indexes
from infrastructure_cache import cache
import inventory
import jobs
import keys
from manual_locks import manual_locker
import storage
import timed_queue


logger = logging.getLogger('mm.infrastructure')

BASE_PORT = config.get('elliptics_base_port', 1024)
CACHE_DEFAULT_PORT = 9999

BASE_STORAGE_PATH = config.get('elliptics_base_storage_path', '/srv/storage/')

RSYNC_MODULE = config.get('restore', {}).get('rsync_use_module') and \
               config['restore'].get('rsync_module')
RSYNC_USER = config.get('restore', {}).get('rsync_user', 'rsync')

RECOVERY_DC_CNF = config.get('infrastructure', {}).get('recovery_dc', {})

logger.info('Rsync module using: %s' % RSYNC_MODULE)
logger.info('Rsync user: %s' % RSYNC_USER)


def dnet_client_backend_command(command):
    def wrapper(host, port, family, backend_id):
        cmd = 'dnet_client backend -r {host}:{port}:{family} {command} --backend {backend_id} --wait-timeout=1000'
        return cmd.format(command=command,
            host=host, port=port, family=family, backend_id=backend_id)
    return wrapper


class Infrastructure(object):

    TASK_SYNC = 'infrastructure_sync'
    TASK_UPDATE = 'infrastructure_update'
    NS_SETTINGS_SYNC = 'ns_settings_sync'

    RSYNC_CMD = ('rsync -rlHpogDt --progress --timeout=1200 '
                 '"{user}@{src_host}:{src_path}data*" "{dst_path}"')
    RSYNC_MODULE_CMD = ('rsync -av --progress --timeout=1200 '
                        '"rsync://{user}@{src_host}/{module}/{src_path}{file_tpl}" '
                        '"{dst_path}"')
    DNET_RECOVERY_DC_CMD = ('dnet_recovery dc {remotes} -g {groups} -D {tmp_dir} '
        '-a {attempts} -b {batch} -l {log} -L {log_level} -n {processes_num} -M')
    DNET_RECOVERY_DC_REMOTE_TPL = '-r {host}:{port}:{family}'

    DNET_DEFRAG_CMD = ('dnet_client backend -r {host}:{port}:{family} '
        'defrag --backend {backend_id}')

    HISTORY_RECORD_AUTOMATIC = 'automatic'
    HISTORY_RECORD_MANUAL = 'manual'
    HISTORY_RECORD_JOB = 'job'

    def __init__(self):

        # actual init happens in 'init' method
        # TODO: return node back to constructor after wrapping
        #       all the code in a 'mastermind' package
        self.node = None
        self.meta_session = None
        self.cache = None

        self.state = {}
        self.__state_lock = threading.Lock()
        self.__tq = timed_queue.TimedQueue()

    def init(self, node, job_finder):
        self.node = node
        self.job_finder = job_finder
        self.meta_session = self.node.meta_session

        self._sync_state()

        self.cache = cache
        cache.init(self.meta_session, self.__tq)

        self.__tq.add_task_in(self.TASK_UPDATE,
            config.get('infrastructure_update_period', 300),
            self._update_state)

        self.ns_settings_idx = \
            indexes.TagSecondaryIndex(keys.MM_NAMESPACE_SETTINGS_IDX,
                                      None,
                                      keys.MM_NAMESPACE_SETTINGS_KEY_TPL,
                                      self.meta_session,
                                      logger=logger,
                                      namespace='namespaces')

        self.ns_settings = {}
        self._sync_ns_settings()

    def _start_tq(self):
        self.__tq.start()

    def get_group_history(self, group_id):
        couples_history = []
        for couple in self.state[group_id]['couples']:
            couples_history.append({'couple': couple['couple'],
                                    'timestamp': couple['timestamp']})
        nodes_history = []
        for node_set in self.state[group_id]['nodes']:

            nodes_history.append(
                {'set': node_set['set'],
                 'timestamp': node_set['timestamp'],
                 'type': self.__node_state_type(node_set)
                })
        return {'couples': couples_history,
                'nodes': nodes_history}

    def node_backend_in_last_history_state(self, group_id, host, port, backend_id):
        if not group_id in self.state:
            raise ValueError('Group {0} history is not found'.format(group_id))

        last_node_set = self.state[group_id]['nodes'][-1]['set']
        for k in last_node_set:
            if len(k) == 2:
                # old style history record
                continue
            nb_host, nb_port, nb_family, nb_backend_id = k[:4]
            if host == nb_host and port == nb_port and backend_id == nb_backend_id:
                return True

        return False

    def _sync_state(self):
        start_ts = time.time()
        try:
            logger.info('Syncing infrastructure state')
            self.__do_sync_state()
            logger.info('Finished syncing infrastructure state, time: {0:.3f}'.format(
                time.time() - start_ts))
        except Exception as e:
            logger.error('Failed to sync infrastructure state, time: {0:.3f}, {1}\n{2}'.format(
                         time.time() - start_ts, e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.TASK_SYNC,
                config.get('infrastructure_sync_period', 60),
                self._sync_state)

    @classmethod
    def __node_state_type(cls, node_state):
        return node_state.get('type',
            node_state.get('manual', False) and cls.HISTORY_RECORD_MANUAL or
                cls.HISTORY_RECORD_AUTOMATIC)

    def __do_sync_state(self):
        group_ids = set()
        with self.__state_lock:

            idxs = self.meta_session.find_all_indexes([keys.MM_GROUPS_IDX])
            for idx in idxs:
                data = idx.indexes[0].data

                state_group = self._unserialize(data)
                # logger.debug('Fetched infrastructure item: %s' %
                #               (state_group,))

                if (self.state.get(state_group['id']) and
                    state_group['id'] in storage.groups):

                    group = storage.groups[state_group['id']]

                    for nodes_state in reversed(state_group['nodes']):
                        if nodes_state['timestamp'] <= self.state[state_group['id']]['nodes'][-1]['timestamp']:
                            break

                        if self.__node_state_type(nodes_state) != self.HISTORY_RECORD_AUTOMATIC:
                            nodes_set = set(nodes_state['set'])
                            for nb in group.node_backends:
                                if not (nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id, nb.base_path) in nodes_set:
                                    logger.info('Removing {0} from group {1} due to manual group detaching'.format(nb, group.group_id))
                                    group.remove_node_backend(nb)
                            group.update_status_recursive()

                self.state[state_group['id']] = state_group
                group_ids.add(state_group['id'])

            for gid in set(self.state.keys()) - group_ids:
                logger.info('Group %d is not found in infrastructure state, '
                            'removing' % gid)
                del self.state[gid]

    @staticmethod
    def _serialize(data):
        return msgpack.packb(data)

    @staticmethod
    def _unserialize(data):
        group_state = msgpack.unpackb(data)
        group_state['nodes'] = list(group_state['nodes'])
        if not 'couples' in group_state:
            group_state['couples'] = []
        group_state['couples'] = list(group_state['couples'])
        return group_state

    @staticmethod
    def _new_group_state(group_id):
        return {
            'id': group_id,
            'nodes': [],
            'couples': [],
        }

    def _update_state(self):
        groups_to_update = []
        start_ts = time.time()
        try:
            logger.info('Updating infrastructure state')

            logger.info('Fetching fresh infrastructure state')
            self.__do_sync_state()
            logger.info('Done fetching fresh infrastructure state, time: {0:.3f}'.format(
                time.time() - start_ts))

            for g in storage.groups.keys():

                group_state = self.state.get(g.group_id,
                                             self._new_group_state(g.group_id))

                storage_nodes = tuple((nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id, nb.base_path)
                                      for nb in g.node_backends if nb.stat)
                storage_couple = (tuple([group.group_id for group in g.couple])
                                  if g.couple is not None else
                                  tuple())

                if not storage_nodes:
                    logger.debug('Storage nodes list for group %d is empty, '
                                  'skipping' % (g.group_id,))
                    continue

                new_nodes = None
                new_couple = None

                with self.__state_lock:
                    if not g.group_id in self.state:
                        # add group to state only if checks succeeded
                        self.state[g.group_id] = group_state

                    cur_group_state = (group_state['nodes'] and
                                       group_state['nodes'][-1]
                                       or {'set': []})

                    state_nodes = tuple(nbs
                                        for nbs in cur_group_state['set'])
                    state_nodes_set = set(state_nodes)

                    # extended storage nodes set which includes newly seen nodes,
                    # do not discard lost nodes
                    # also filter out old history nodes
                    ext_storage_nodes = (storage_nodes + tuple(
                        nb for nb in state_nodes
                            if nb not in storage_nodes and len(nb) != 2))

                    if set(ext_storage_nodes) != state_nodes_set:
                        logger.info('Group %d info does not match,'
                                     'last state: %s, current state: %s' %
                                     (g.group_id, state_nodes, ext_storage_nodes))
                        new_nodes = ext_storage_nodes

                    cur_couple_state = (group_state['couples'] and
                                        group_state['couples'][-1]
                                        or {'couple': tuple()})
                    state_couple = cur_couple_state['couple']

                    if storage_couple and set(state_couple) != set(storage_couple):
                        logger.info('Group %d couple does not match,'
                                     'last state: %s, current state: %s' %
                                     (g.group_id, state_couple, storage_couple))
                        new_couple = storage_couple

                    if new_nodes or new_couple:
                        try:
                            self._update_group(g.group_id, new_nodes, new_couple)
                        except Exception as e:
                            logger.error('Failed to update infrastructure state for group %s: %s\n%s' %
                                (g.group_id, e, traceback.format_exc()))
                            pass


            logger.info('Finished updating infrastructure state, time: {0:.3f}'.format(
                time.time() - start_ts))
        except Exception as e:
            logger.error('Failed to update infrastructure state, time: {0:.3f}, {1}\n{2}'.format(
                            time.time() - start_ts, e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.TASK_UPDATE,
                config.get('infrastructure_update_period', 300),
                self._update_state)

    def _sync_ns_settings(self):
        try:
            logger.debug('fetching all namespace settings')
            start = time.time()
            for data in self.ns_settings_idx:
                self.__do_sync_ns_settings(data, start)
        except Exception as e:
            logger.error('Failed to sync ns settings: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.NS_SETTINGS_SYNC,
                config.get('infrastructure_ns_settings_sync_period', 60),
                self._sync_ns_settings)

    def sync_single_ns_settings(self, namespace):
        logger.debug('fetching namespace {0} settings'.format(namespace))
        start_ts = time.time()
        self.__do_sync_ns_settings(self.ns_settings_idx[namespace], start_ts)

    def __do_sync_ns_settings(self, data, start_ts):
        settings = msgpack.unpackb(data)
        logger.debug('fetched namespace settings for "{0}" '
            '({1:.3f}s)'.format(settings['namespace'], time.time() - start_ts))
        ns = settings['namespace']
        del settings['namespace']
        self.ns_settings[ns] = settings

    def set_ns_settings(self, namespace, settings):

        logger.debug('saving settings for namespace "{0}": {1}'.format(
            namespace, settings))

        settings['namespace'] = namespace
        start = time.time()

        self.ns_settings_idx[namespace] = msgpack.packb(settings)
        if not namespace in self.ns_settings:
            self.ns_settings_idx.set_tag(namespace)

        logger.debug('namespace "{0}" settings saved to index '
            '({1:.4f}s)'.format(namespace, time.time() - start))

        del settings['namespace']
        self.ns_settings[namespace] = settings

    def _update_group(self, group_id, new_nodes=None, new_couple=None, record_type=None):
        group = self.state[group_id]
        if new_nodes is not None:
            new_nodes_state = {'set': new_nodes,
                               'timestamp': time.time()}
            new_nodes_state['type'] = record_type or self.HISTORY_RECORD_AUTOMATIC

            group['nodes'].append(new_nodes_state)

        if new_couple is not None:
            new_couples_state = {'couple': new_couple,
                                 'timestamp': time.time()}
            group['couples'].append(new_couples_state)

        eid = self.meta_session.transform(keys.MM_ISTRUCT_GROUP % group_id)
        logger.info('Updating state for group %s' % group_id)
        self.meta_session.update_indexes(eid, [keys.MM_GROUPS_IDX],
                                              [self._serialize(group)]).get()

    def detach_node(self, group_id, host, port, backend_id, record_type=None):
        with self.__state_lock:
            if not group_id in self.state:
                raise ValueError('Node backend {0}:{1}/{2} not found in '
                    'group {3} history state'.format(host, port, backend_id, group_id))

            group_state = self.state[group_id]
            state_nodes = list(group_state['nodes'][-1]['set'][:])

            logger.info('{0}'.format(state_nodes))
            for i, state_node in enumerate(state_nodes):
                state_host, state_port, state_family, state_backend_id = state_node[:4]
                if state_host == host and state_port == port and state_backend_id == backend_id:
                    logger.debug('Removing node backend {0}:{1}/{2} from '
                        'group {3} history state'.format(host, port, backend_id, group_id))
                    del state_nodes[i]
                    break
            else:
                raise ValueError('Node backend {0}:{1}/{2} not found in '
                    'group {3} history state'.format(host, port, backend_id, group_id))

            self._update_group(group_id, state_nodes, None,
                record_type=record_type or self.HISTORY_RECORD_MANUAL)


    def move_group_cmd(self, src_host, src_port=None, src_family=2,
                       src_path=None,  dst_port=None, dst_path=None,
                       user=None, file_tpl='data*'):
        cmd_src_path = src_path
        if RSYNC_MODULE:
            cmd = self.RSYNC_MODULE_CMD.format(
                user=RSYNC_USER,
                module=RSYNC_MODULE,
                src_host=src_host if src_family != 10 else '[{0}]'.format(src_host),
                src_path=cmd_src_path.replace(BASE_STORAGE_PATH, ''),
                dst_path=dst_path,
                file_tpl=file_tpl)
        else:
            cmd = self.RSYNC_CMD.format(
                user=user,
                src_host=src_host if src_family != 10 else '[{0}]'.format(src_host),
                src_path=cmd_src_path,
                dst_path=dst_path,
                file_tpl=file_tpl)
        return cmd

    @h.concurrent_handler
    def start_node_cmd(self, request):

        host, port, family = request[:3]

        cmd = inventory.node_start_command(host, port, family)

        if cmd is None:
            raise RuntimeError('Node start command is not provided '
                'by inventory implementation')

        logger.info('Command for starting elliptics node {0}:{1} '
            'was requested: {2}'.format(host, port, cmd))

        return cmd

    @h.concurrent_handler
    def shutdown_node_cmd(self, request):

        host, port = request[:2]

        node_addr = '{0}:{1}'.format(host, port)

        if not node_addr in storage.nodes:
            raise ValueError("Node {0} doesn't exist".format(node_addr))

        node = storage.nodes[node_addr]

        cmd = inventory.node_shutdown_command(node.host, node.port, node.family)
        logger.info('Command for shutting down elliptics node {0} '
            'was requested: {1}'.format(node_addr, cmd))

        return cmd

    _enable_node_backend_cmd = staticmethod(dnet_client_backend_command('enable'))
    _disable_node_backend_cmd = staticmethod(dnet_client_backend_command('disable'))
    _make_readonly_node_backend_cmd = staticmethod(dnet_client_backend_command('make_readonly'))
    _make_writable_node_backend_cmd = staticmethod(dnet_client_backend_command('make_writable'))


    @h.concurrent_handler
    def enable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id)

        cmd = self._enable_node_backend_cmd(host, port, family, backend_id)

        if cmd is None:
            raise RuntimeError('Node backend start command is not provided '
                'by inventory implementation')

        logger.info('Command for starting elliptics node {0} '
            'was requested: {1}'.format(nb_addr, cmd))

        return cmd

    @h.concurrent_handler
    def disable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

        if not nb_addr in storage.node_backends:
            raise ValueError("Node backend {0} doesn't exist".format(nb_addr))

        nb = storage.node_backends[nb_addr]

        cmd = self._disable_node_backend_cmd(
            nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)
        logger.info('Command for shutting down elliptics node backend {0} '
            'was requested: {1}'.format(nb_addr, cmd))

        return cmd

    def make_readonly_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

        if not nb_addr in storage.node_backends:
            raise ValueError("Node backend {0} doesn't exist".format(nb_addr))

        nb = storage.node_backends[nb_addr]

        cmd = self._make_readonly_node_backend_cmd(
            nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)
        logger.info('Command for making elliptics node backend {0} read-only '
            'was requested: {1}'.format(nb_addr, cmd))

        return cmd

    def make_writable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

        if not nb_addr in storage.node_backends:
            raise ValueError("Node backend {0} doesn't exist".format(nb_addr))

        nb = storage.node_backends[nb_addr]

        cmd = self._make_writable_node_backend_cmd(
            nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)
        logger.info('Command for making elliptics node backend {0} writable '
            'was requested: {1}'.format(nb_addr, cmd))

        return cmd

    @h.concurrent_handler
    def reconfigure_node_cmd(self, request):

        host, port, family = request[:3]

        node_addr = '{0}:{1}'.format(host, port)

        if not node_addr in storage.nodes:
            raise ValueError("Node {0} doesn't exist".format(node_addr))

        cmd = self._reconfigure_node_cmd(host, port, family)

        logger.info('Command for reconfiguring elliptics node {0} '
            'was requested: {1}'.format(node_addr, cmd))

        return cmd

    def _reconfigure_node_cmd(self, host, port, family):

        cmd = inventory.node_reconfigure(host, port, family)

        if cmd is None:
            raise RuntimeError('Node reconfiguration command is not provided '
                'by inventory implementation')
        return cmd

    @h.concurrent_handler
    def recover_group_cmd(self, request):

        try:
            group_id = int(request[0])
            if not group_id in storage.groups:
                raise ValueError
        except (ValueError, TypeError):
            raise ValueError('Group {0} is not found'.format(request[0]))

        cmd = self._recover_group_cmd(group_id)

        logger.info('Command for dc recovery for group {0} '
            'was requested: {1}'.format(group_id, cmd))

        return cmd

    def _recover_group_cmd(self, group_id):
        group = storage.groups[group_id]
        if not group.couple:
            raise ValueError('Group {0} is not coupled'.format(group_id))

        remotes = []
        for g in group.couple.groups:
            for nb in g.node_backends:
                remotes.append(self.DNET_RECOVERY_DC_REMOTE_TPL.format(
                    host=nb.node.host.addr,
                    port=nb.node.port,
                    family=nb.node.family,))

        cmd = self.DNET_RECOVERY_DC_CMD.format(
            remotes=' '.join(remotes),
            groups=','.join(str(g) for g in group.couple.groups),
            tmp_dir=RECOVERY_DC_CNF.get('tmp_dir',
                '/var/tmp/dnet_recovery_dc_{group_id}').format(group_id=group_id),
            attempts=RECOVERY_DC_CNF.get('attempts', 1),
            batch=RECOVERY_DC_CNF.get('batch', 2000),
            log=RECOVERY_DC_CNF.get('log', 'dnet_recovery.log').format(group_id=group_id),
            log_level=RECOVERY_DC_CNF.get('log_level', 1),
            processes_num=len(group.couple.groups) - 1 or 1)

        return cmd

    @h.concurrent_handler
    def defrag_node_backend_cmd(self, request):

        try:
            host, port, family, backend_id = request[:4]
            port, family, backend_id = map(int, (port, family, backend_id))
            node_backend_str = '{0}:{1}/{2}'.format(host, port, backend_id)
            node_backend = storage.node_backends[node_backend_str]
        except (ValueError, TypeError, KeyError):
            raise ValueError('Node backend {0} is not found'.format(node_backend_str))

        cmd = self._defrag_node_backend_cmd(
            node_backend.node.host.addr,
            node_backend.node.port,
            node_backend.node.family,
            node_backend.backend_id)

        logger.info('Command for node backend {0} defragmentation '
            'was requested: {1}'.format(node_backend, cmd))

        return cmd

    def _defrag_node_backend_cmd(self, host, port, family, backend_id):
        cmd = self.DNET_DEFRAG_CMD.format(
            host=host, port=port, family=family, backend_id=backend_id)
        return cmd

    @h.concurrent_handler
    def search_history_by_path(self, request):
        params = request[0]

        try:
            host = params['host']
            path = params['path']
        except KeyError:
            raise ValueError('Host and path parameters are required')

        entries = []

        if not path.startswith('^'):
            path = '^' + path
        if not path.endswith('$'):
            path = path + '$'
        path = path.replace('*', '.*')

        start_idx = 0
        if params.get('last', False):
            start_idx = -1

        for group_id, group_history in self.state.iteritems():
            for node_set in group_history['nodes'][start_idx:]:
                ts = node_set['timestamp']
                for node in node_set['set']:

                    node_path = node[4]

                    if not node_path:
                        continue

                    if re.match(path, node_path) is not None and node[0] == host:
                        entries.append((ts, group_id, node_set))
                        break

        entries.sort(key=lambda e: e[0])
        result = []

        for entry in entries:
            result.append({'group': entry[1],
                           'set': entry[2]['set'],
                           'timestamp': entry[0],
                           'type': self.__node_state_type(entry[2])})

        return result

    def cluster_tree(self, namespace=None):
        nodes = {}
        root = {}
        hosts = []

        if namespace:
            for couple in storage.couples:
                try:
                    if couple.namespace == namespace:
                        hosts.extend([nb.node.host for g in couple for nb in g.node_backends])
                except ValueError:
                    continue
            hosts = list(set(hosts))
        else:
            hosts = storage.hosts.keys()

        for host in hosts:
            try:
                tree_node = deepcopy(host.parents)
            except CacheUpstreamError:
                logger.warn('Skipping {} because of cache failure'.format(host))
                continue
            new_child = None
            while True:
                parts = [tree_node['name']]
                parent = tree_node
                while 'parent' in parent:
                    parent = parent['parent']
                    parts.append(parent['name'])
                full_path = '|'.join(reversed(parts))
                tree_node['full_path'] = full_path

                type_nodes = nodes.setdefault(tree_node['type'], {})
                cur_node = type_nodes.get(tree_node['full_path'], {'name': tree_node['name'],
                                                                   'full_path': tree_node['full_path'],
                                                                   'type': tree_node['type']})

                if new_child:
                    cur_node.setdefault('children', []).append(new_child)
                    new_child = None

                if not tree_node['full_path'] in type_nodes:
                    type_nodes[tree_node['full_path']] = cur_node
                    new_child = cur_node

                if not 'parent' in tree_node:
                    if not root:
                        root = nodes[tree_node['type']]
                    break
                tree_node = tree_node['parent']

        tree = {'type': 'root', 'name': 'root',
                'children': root.values()}
        return tree, nodes

    def filtered_cluster_tree(self, types, namespace=None):
        tree, nodes = self.cluster_tree(namespace=namespace)

        def move_allowed_children(node, dest):
            for child in node.get('children', []):
                if child['type'] not in types:
                    move_allowed_children(child, dest)
                else:
                    dest['children'].append(child)

        def flatten_tree(root):
            for child in root.get('children', [])[:]:
                if child['type'] not in types:
                    move_allowed_children(child, root)
                    root['children'].remove(child)
                else:
                    flatten_tree(child)

        flatten_tree(tree)

        for k in nodes.keys():
            if not k in types:
                del nodes[k]

        nodes['hdd'] = {}

        for nb in storage.node_backends:

            try:
                full_path = nb.node.host.full_path
            except CacheUpstreamError:
                logger.warn('Skipping {} because of cache failure'.format(
                    nb.node.host))
                continue

            if not full_path in nodes['host']:
                logger.warn('Host {0} is not found in cluster tree'.format(full_path))
                continue
            if nb.stat is None:
                continue

            fsid = str(nb.stat.fsid)
            fsid_full_path = full_path + '|' + fsid
            if not fsid_full_path in nodes['hdd']:
                hdd_node = {
                    'type': 'hdd',
                    'name': fsid,
                    'full_path': fsid_full_path,
                }
                nodes['hdd'][fsid_full_path] = hdd_node
                nodes['host'][full_path].setdefault('children', []).append(hdd_node)

        return tree, nodes

    def update_groups_list(self, root):
        if not 'children' in root:
            return root.setdefault('groups', set())
        root['groups'] = reduce(operator.or_,
            (self.update_groups_list(child) for child in root.get('children', [])),
            set())
        return root['groups']

    def account_ns_groups(self, nodes, groups):
        for group in groups:
            for nb in group.node_backends:
                try:
                    hdd_path = nb.node.host.full_path + '|' + str(nb.stat.fsid)
                except CacheUpstreamError:
                    logger.warn('Skipping {} because of cache failure'.format(
                        nb.node.host))
                    continue
                if not hdd_path in nodes['hdd']:
                    # when building cluster tree every host resolve operation
                    # failed for this hdd
                    continue
                nodes['hdd'][hdd_path]['groups'].add(group.group_id)

    def account_ns_couples(self, tree, nodes, namespace):

        for hdd in nodes['hdd'].itervalues():
            hdd['groups'] = set()

        for couple in storage.couples:
            try:
                ns = couple.namespace
            except ValueError:
                continue

            if namespace != ns:
                continue

            if couple.status != storage.Status.OK:
                continue

            self.account_ns_groups(nodes, couple.groups)

        self.update_groups_list(tree)

    def ns_current_state(self, nodes, types):
        ns_current_state = {}
        for node_type in types:
            ns_current_state[node_type] = {'nodes': {},
                                           'avg': 0}
            for child in nodes[node_type].itervalues():
                if not 'groups' in child:
                    logger.error('No groups in child {0}'.format(child))
                ns_current_state[node_type]['nodes'][child['full_path']] = len(child.get('groups', []))
            ns_current_state[node_type]['avg'] = (
                float(sum(ns_current_state[node_type]['nodes'].values())) /
                len(nodes[node_type]))
        return ns_current_state

    def groups_units(self, groups, types):
        units = {}

        for group in groups:
            if group.group_id in units:
                continue
            for nb in group.node_backends:

                try:
                    parent = nb.node.host.parents
                except CacheUpstreamError:
                    logger.warn('Skipping {} because of cache failure'.format(
                        nb.node.host))
                    continue

                nb_units = {'root': 'root'}
                units.setdefault(group.group_id, [])

                parts = []
                cur_node = parent
                while cur_node:
                    parts.insert(0, cur_node['name'])
                    cur_node = cur_node.get('parent')

                while parent:
                    if parent['type'] in types:
                        full_path = '|'.join(reversed(parts))
                        nb_units[parent['type']] = '|'.join(parts)
                    parts.pop()
                    parent = parent.get('parent')

                nb_units['hdd'] = (nb_units['host'] + '|' +
                    str(nb.stat.fsid))

                units[group.group_id].append(nb_units)

        return units

    def get_group_ids_in_service(self):
        group_ids_in_service = []
        if not self.job_finder:
            return group_ids_in_service
        for job in self.job_finder.jobs(statuses=jobs.Job.ACTIVE_STATUSES):
            group_ids_in_service.extend(job._involved_groups)
        return group_ids_in_service

    def get_good_uncoupled_groups(self,
        max_node_backends=None,
        including_in_service=False,
        status=None,
        types=None):

        suitable_groups = []
        locked_hosts = manual_locker.get_locked_hosts()
        in_service = set()
        if not including_in_service and self.job_finder:
            in_service = set(self.job_finder.get_uncoupled_groups_in_service())
        logger.debug('Groups in service - total {0}: {1}'.format(
            len(in_service), in_service))
        for group in storage.groups.keys():
            if Infrastructure.is_uncoupled_group_good(group,
                    locked_hosts,
                    types or (storage.Group.TYPE_DATA,),
                    max_node_backends=max_node_backends,
                    in_service=in_service,
                    status=status):
                suitable_groups.append(group)

        return suitable_groups

    @staticmethod
    def is_uncoupled_group_good(group, locked_hosts,
        types, max_node_backends=None, in_service=None, status=None):

        if group.couple is not None:
            return False

        if in_service and group.group_id in in_service:
            return False

        if not len(group.node_backends):
            return False

        status = status or storage.Status.INIT
        if group.status != status:
            return False

        for nb in group.node_backends:
            if nb.status != storage.Status.OK:
                return False
            if nb.node.host in locked_hosts:
                return False

        if group.type not in types:
            return False

        if max_node_backends and len(group.node_backends) > max_node_backends:
            return False

        return True


infrastructure = Infrastructure()
