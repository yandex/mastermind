import keys
import os.path
import time
import traceback

from cocaine.logging import Logger
import elliptics
import msgpack

from config import config
import storage
import timed_queue


logging = Logger()


BASE_PORT = config.get('elliptics_base_port', 1024)
CACHE_DEFAULT_PORT = 9999

BASE_STORAGE_PATH = config.get('elliptics_base_storage_path', '/srv/storage/')
CACHE_DEFAULT_PATH = '/srv/cache/'


class Infrastructure(object):

    TASK_SYNC = 'infrastructure_sync'
    TASK_UPDATE = 'infrastructure_update'

    RSYNC_CMD = ('rsync -rlHpogDt --progress '
                 '{user}@{src_host}:{src_path}data* {dst_path}')

    def __init__(self):

        # actual init happens in 'init' method
        # TODO: return node back to constructor after wrapping
        #       all the code in a 'mastermind' package
        self.node = None
        self.meta_session = None

        self.state = {}
        self.sync_ts = None
        self.state_valid_time = config.get('infrastructure_state_valid_time',
                                           120)

        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

    def init(self, node):
        self.node = node
        self.meta_session = self.node.meta_session

        self.sync_state()
        self.__tq.add_task_in(self.TASK_UPDATE,
            config.get('infrastructure_update_period', 300),
            self.update_state)

    def get_group_history(self, group_id):
        history = []
        for node_set in self.state[group_id]['nodes']:
            history.append({'set': [node + (port_to_path(node[1]),)
                                    for node in node_set['set']],
                            'timestamp': node_set['timestamp']})
        return history

    def sync_state(self):
        try:
            logging.info('Syncing infrastructure state')
            group_ids = set()
            idxs = self.meta_session.find_all_indexes([keys.MM_GROUPS_IDX])
            for idx in idxs:
                data = idx.indexes[0].data

                state_group = self.unserialize(data)
                logging.debug('Fetched infrastructure item: %s' %
                              (state_group,))

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
        except BaseException as e:
            logging.error('Bad infrastructure shit: %s' % (e,))
        finally:
            self.__tq.add_task_in(self.TASK_SYNC,
                config.get('infrastructure_sync_period', 60),
                self.sync_state)

    @staticmethod
    def serialize(data):
        return msgpack.packb(data)

    @staticmethod
    def unserialize(data):
        group_state = msgpack.unpackb(data)
        group_state['nodes'] = list(group_state['nodes'])
        return group_state

    @staticmethod
    def new_group_state(group_id):
        return {
            'id': group_id,
            'nodes': [],
        }

    def update_state(self):
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
                                             self.new_group_state(g.group_id))

                storage_nodes = tuple((node.host.addr, node.port)
                                      for node in g.nodes)

                if not storage_nodes:
                    logging.debug('Storage nodes list for group %d is empty, '
                                  'skipping' % (g.group_id,))
                    continue

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
                    self.update_group(g.group_id, ext_storage_nodes)

            logging.info('Finished updating infrastructure state')
        except Exception as e:
            logging.error('Failed to update infrastructure state: %s\n%s' %
                          (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.TASK_UPDATE,
                config.get('infrastructure_update_period', 300),
                self.update_state)

    def update_group(self, group_id, new_nodes):
        group = self.state[group_id]
        group['nodes'].append({'set': new_nodes,
                               'timestamp': time.time()})

        eid = elliptics.Id(keys.MM_ISTRUCT_GROUP % group_id)
        logging.info('Updating state for group %s' % group_id)
        self.meta_session.update_indexes(eid, [keys.MM_GROUPS_IDX],
                                              [self.serialize(group)])

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

            source_group = group_candidates[0]
            source_node = source_group.nodes[0]

            state = self.get_group_history(group.group_id)[-1]['set']
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

            cmd = self.RSYNC_CMD.format(user=user,
                src_host=source_node.host.addr,
                src_path=port_to_path(source_node.port),
                dst_path=dest or port_to_path(port))

        except ValueError as e:
            warns.append(e.message)
            logging.info('Restore cmd for group %s failed, warns: %s' %
                         (group_id, warns))
            return '', warns

        logging.info('Restore cmd for group %s, warns: %s, cmd %s' %
                     (group_id, warns, cmd))
        return cmd, warns


def port_to_path(port):
    assert port >= BASE_PORT
    if port == CACHE_DEFAULT_PORT:
        return CACHE_DEFAULT_PATH
    return os.path.join(BASE_STORAGE_PATH, str(port - BASE_PORT) + '/')
