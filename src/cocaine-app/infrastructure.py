import keys
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


class Infrastructure(object):

    TASK_SYNC = 'infrastructure_sync'
    TASK_UPDATE = 'infrastructure_update'

    RSYNC_CMD = 'rsync -rlHpogDt --progress {src_host}:{src_path} {dst_path}'

    def __init__(self, node):
        self.node = node
        self.meta_session = self.node.meta_session
        self.state = {}

        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

        self.sync_state()
        self.__tq.add_task_in(self.TASK_UPDATE,
            config.get('infrastructure_update_period', 300),
            self.update_state)

    def get_group_history(self, group_id):
        return self.state[group_id]['nodes']

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
            for g in storage.groups.keys():

                self.state.setdefault(g.group_id,
                                      self.new_group_state(g.group_id))

                cur_group_state = (self.state[g.group_id]['nodes'] and
                                   self.state[g.group_id]['nodes'][-1]
                                   or {'set': []})

                state_nodes = tuple(nodes
                                    for nodes in cur_group_state['set'])
                storage_nodes = tuple((node.host.addr, node.port)
                                      for node in g.nodes)

                logging.debug('Comparing %s and %s' %
                              (storage_nodes, state_nodes))

                if set(storage_nodes) != set(state_nodes):
                    logging.info('Group %d info does not match,'
                                 'last state: %s, current state: %s' %
                                 (g.group_id, state_nodes, storage_nodes))
                    self.update_group(g.group_id, storage_nodes)

            logging.info('Finished updating infrastructure state')
        except Exception as e:
            logging.error('Failed to update infrastructure state: %s\n%s' %
                          (e, traceback.format_exc()))
            # maybe add some tiny weeny random?
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

        candidates = set()
        warns = []

        try:
            group = storage.groups[group_id]
            if group.couple:
                candidates.add(group.couple)
                for g in group.couple:
                    if g == group:
                        continue
                    if not group in g.couple:
                        warns.append('Group %s is not found in couple of group %s' %
                                     (group.group_id, g.group_id))
                    else:
                        candidates.add(g.couple)
            else:
                candidates.update(c for c in storage.couples if group in c)

            if not candidates:
                raise ValueError('Couples containing group being restored not found')

            couple = candidates.pop()
            if len(candidates) > 1:
                warns.append('More than one couple candidate '
                             'for group restoration: %s' % (candidates,))
                warns.append('Selected couple: %s' % (couple,))

            group_candidates = []
            for g in couple:
                if g == group:
                    continue
                if g.status == storage.Status.INIT:
                    warns.append('Cannot use group %s, status: %s' %
                                 (g.group_id, g.status))
                else:
                    group_candidates.append(g)

            if not group_candidates:
                raise ValueError('No symmetric groups to restore from')

            source_group = group_candidates[0]
            source_node = source_group.nodes[0]
            logging.info('Group %s is restored from group %s' %
                         (group, source_group))

            state = self.get_group_history(group.group_id)[-1]['set']
            if (group.nodes[0].host.addr != state[0][0] or
                group.nodes[0].port != int(state[0][1])):
                warns.append('%s == %s, %s' % (group.nodes[0].host.addr, state[0][0], group.nodes[0].port != state[0][0]))
                warns.append('%s' % (type(group.nodes[0].host),))
                warns.append('%s' % (type(state[0][0]),))
                warns.append('Last history state does not match '
                             'current state, history will be used: '
                             'history %s, current %s' %
                             (state, group.nodes[0]))

            warns.append('%s' % len(source_group.nodes))
            warns.append('%s' % state)
            if len(source_group.nodes) > 1 or len(state) > 1:
                raise ValueError('Do not know how to restore group '
                                 'with more than one node')

            logging.info('state: %s' % state)

            cmd = self.RSYNC_CMD.format(src_host=source_node.host.addr,
                                        src_path=port_to_srv(source_node.port),
                                        dst_path=port_to_srv(state[0][1]))

        except ValueError as e:
            warns.append(e.message)
            return '', warns

        return cmd, warns


def port_to_srv(port):
    assert port >= BASE_PORT + 1
    if port == CACHE_DEFAULT_PORT:
        return '/srv/cache'
    return '/srv/%d' % (port - BASE_PORT)
