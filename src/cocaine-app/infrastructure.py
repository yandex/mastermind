from copy import deepcopy
import logging
import operator
import re
import threading
import time
import traceback
import uuid

from errors import CacheUpstreamError
import helpers as h
from history import (
    GroupHistory,
    GroupNodeBackendsSetRecord,
    GroupNodeBackendsSet,
    GroupNodeBackendRecord,
    GroupCoupleRecord,
    GroupStateRecord
)
from infrastructure_cache import cache
import inventory
import jobs
from manual_locks import manual_locker
from mastermind_core.config import config
from mastermind_core.max_group import max_group_manager
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
LRC_CONVERT_DC_CNF = config.get('infrastructure', {}).get('lrc_convert', {})
LRC_VALIDATE_DC_CNF = config.get('infrastructure', {}).get('lrc_validate', {})

logger.info('Rsync module using: %s' % RSYNC_MODULE)
logger.info('Rsync user: %s' % RSYNC_USER)


DNET_CLIENT_BACKEND_CMD_TPL = (
    'dnet_client backend -r {host}:{port}:{family} '
    '{dnet_client_command} --backend {backend_id} --wait-timeout=1000'
)


def dnet_client_backend_command(command):
    def wrapper(host, port, family, backend_id):
        return DNET_CLIENT_BACKEND_CMD_TPL.format(
            dnet_client_command=command,
            host=host,
            port=port,
            family=family,
            backend_id=backend_id
        )
    return wrapper


class Infrastructure(object):

    TASK_SYNC = 'infrastructure_sync'
    TASK_UPDATE = 'infrastructure_update'

    RSYNC_CMD = ('rsync -rlHpogDt --progress --timeout=1200 '
                 '"{user}@{src_host}:{src_path}data*" "{dst_path}"')
    RSYNC_MODULE_CMD = ('rsync -av --progress --timeout=1200 '
                        '"rsync://{user}@{src_host}/{module}/{src_path}{file_tpl}" '
                        '"{dst_path}"')
    DNET_RECOVERY_DC_CMD = (
        'dnet_recovery dc {remotes} -g {groups} -D {tmp_dir} '
        '-a {attempts} -b {batch} -l {log} -L {log_level} -n {processes_num} -M '
        '-T {trace_id} {json_stats}'
    )
    REMOTE_TPL = '-r {host}:{port}:{family}'

    DNET_DEFRAG_CMD = (
        'dnet_client backend -r {host}:{port}:{family} '
        'defrag --backend {backend_id} --wait-timeout=1000'
    )

    LRC_CONVERT_CMD = (
        'lrc_convert {remotes} --src-groups {src_groups} --dst-groups {dst_groups} '
        '--part-size {part_size} --scheme {scheme} --log {log} --log-level {log_level} '
        '--tmp {tmp_dir} --attempts {attempts} --trace-id {trace_id} '
        '--data-flow-rate {data_flow_rate} --wait-timeout {wait_timeout}'
    )

    LRC_VALIDATE_CMD = (
        'lrc_validate {remotes} --src-groups {src_groups} --dst-groups {dst_groups} '
        '--part-size {part_size} --scheme {scheme} --log {log} --log-level {log_level} '
        '--tmp {tmp_dir} --attempts {attempts} --trace-id {trace_id} '
        '--data-flow-rate {data_flow_rate} --wait-timeout {wait_timeout}'
    )

    TTL_CLEANUP_CMD = (
        'mds_cleanup --groups {groups} --iterate-group {iter_group} '
        '--log {log} --log-level {log_level} --tmp {tmp_dir} --trace-id {trace_id} '
        '--wait-timeout {wait_timeout} --attempts {attempts} --batch-size {batch_size} '
        '--nproc {nproc} {safe} {remotes} --elliptics-log-level error '
        '{remove_type} {tskv}'
    )

    def __init__(self):

        # actual init happens in 'init' method
        # TODO: return node back to constructor after wrapping
        #       all the code in a 'mastermind' package
        self.node = None
        self.cache = None
        self._sync_ts = int(time.time())

        self._groups_to_update = set()
        self._groups_to_update_lock = threading.Lock()

        self.__tq = timed_queue.TimedQueue()

    def init(self, node, job_finder, group_history_finder, namespaces_settings):
        self.node = node
        self.job_finder = job_finder
        self.group_history_finder = group_history_finder
        self.namespaces_settings = namespaces_settings

        if self.group_history_finder:
            self._sync_state()

        self.cache = cache
        cache.init(self.__tq)

    def schedule_history_update(self):
        if not self.group_history_finder:
            return
        try:
            self.__tq.add_task_in(
                task_id=self.TASK_UPDATE,
                secs=0,
                function=self._update_state
            )
        except ValueError:
            # task is already scheduled
            pass

    def _start_tq(self):
        self.__tq.start()

    def get_group_history(self, group_id):
        if not self.group_history_finder:
            raise ValueError('History for group {} is not found'.format(group_id))
        group_history = self.group_history_finder.group_history(group_id)
        if group_history is None:
            raise ValueError('History for group {} is not found'.format(group_id))
        return group_history

    def get_group_histories(self, group_ids=None):
        for gh in self.group_history_finder.group_histories(group_ids=group_ids):
            yield gh

    def node_backend_in_last_history_state(self, group_id, hostname, port, backend_id):
        group_history = self.get_group_history(group_id)

        last_node_set = group_history.nodes[-1].set
        for k in last_node_set:
            if hostname == k.hostname and port == k.port and backend_id == k.backend_id:
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
            self.__tq.add_task_in(
                self.TASK_SYNC,
                config.get('infrastructure_sync_period', 60),
                self._sync_state
            )

    def __do_sync_state(self):
        """
        Apply all new non-automatic history records

        To sync group state among all mastermind workers we apply the following strategy:

            any worker can add a new state record (couple or node backends set) and
            mark it with a non-automatic type (manual, job, etc.). Such state changes
            are found and applied by all other workers periodically.

        This method implements the described strategy by performing search and applying
        records that are found.
        """
        new_ts = int(time.time())

        types_to_sync = (
            GroupStateRecord.HISTORY_RECORD_MANUAL,
            GroupStateRecord.HISTORY_RECORD_JOB
        )

        for group_history in self.group_history_finder.search_by_history_record(
            type=types_to_sync,
            start_ts=self._sync_ts
        ):
            logger.debug('Found updated group history for group {}'.format(group_history.group_id))
            if group_history.group_id not in storage.groups:
                continue
            group = storage.groups[group_history.group_id]

            # this loop responsible only for removing excess node_backend's,
            # since adding node_backend's will proceed automatically
            # during mastermind periodical updates
            for node_backends_set in group_history.nodes:
                # top threshold is checked due to mongo optimization: using bottom threshold only
                # leads to mongo using index interval [<bottom_threshold>, inf+], which matches a
                # lot less number of records than [inf-, <top_threshold>] (apparently mongo can use
                # only one interval end for range queries)
                if not self._sync_ts <= node_backends_set.timestamp < new_ts:
                    continue
                if node_backends_set.type not in types_to_sync:
                    continue

                for nb in group.node_backends:
                    for nb_record in node_backends_set.set:
                        if (
                            nb_record.hostname == nb.node.host.hostname and
                            nb_record.port == nb.node.port and
                            nb_record.family == nb.node.family and
                            nb_record.backend_id == nb.backend_id and
                            nb_record.path == nb.base_path
                        ):
                            break
                    else:
                        logger.info(
                            'Removing {} from group {} due to manual group detaching'.format(
                                nb, group.group_id
                            )
                        )
                        group.remove_node_backend(nb)
                        group.update_status_recursive()

            # another instance of mastermind can destroy couple;
            # now the following loop responsible only for destroying that couple
            # in the state of this mastermind

            # special case: if the first record in the history is `no couple`,
            # then we will break the current couple of the group
            previous_couple = group.couple.as_tuple() if group.couple else ()
            for couple_record in group_history.couples:
                if (
                    not self._sync_ts <= couple_record.timestamp < new_ts or

                    couple_record.type not in types_to_sync
                ):
                    previous_couple = couple_record.couple
                    continue

                if not couple_record.couple and group.couple:
                    if previous_couple == group.couple.as_tuple():
                        logger.info('Breaking couple {} due to manual couple break'.format(
                            previous_couple
                        ))
                        group.couple.destroy()
                    else:
                        logger.error(
                            'Applying manual couple break from history is impossible: '
                            'we expect that group {} couple with couple {}, '
                            'but instead it coupled with couple {}'.format(
                                group, previous_couple, group.couple.as_tuple()
                            )
                        )

                previous_couple = couple_record.couple

        self._sync_ts = new_ts

    def update_group_history(self, group):
        """
        Add group to update group history queue

        This method should be called if there is a chance that group has changed
        it's state and it's group history record needs to be updated.
        This can happen when:
            - node backend is added to group;
            - existing node backend changes its path;
            - group is added to couple.
        """
        with self._groups_to_update_lock:
            self._groups_to_update.add(group)

    def _new_group_history(self, group_id):
        gh = GroupHistory.new(group_id=group_id)
        gh.collection = self.group_history_finder.collection
        return gh

    def _new_group_node_backends_set_record(self, group, group_history):
        """
        Create new node backends set record for group history

        New record is not allowed to exclude any of the backends that are recorded
        in the history's most recent set. The sole purpose of new record generation
        is to extend mentioned set with a new backend that wasn't already there but
        should be according to the current group's set.

        The workflow is following:

            1) if group's current state contains node backends that are not found
            in the most recent history record, a new record should be created containing
            all node backends from the most recent history record as well as node backends
            from the current group's state;

            2) if group's current state contains no node backends, new record should not
            be created (this can happen if the group is temporarily unavailable).

        Returns:
            - new node backends set record to save;
            - <None> if node backends set should not be updated.
        """

        current_state_node_backends_set = GroupNodeBackendsSet(
            GroupNodeBackendRecord(**{
                'hostname': nb.node.host.hostname,
                'port': nb.node.port,
                'family': nb.node.family,
                'backend_id': nb.backend_id,
                'path': nb.base_path,
            })
            for nb in group.node_backends if nb.stat
        )

        if not current_state_node_backends_set:
            return None

        if group_history.nodes:
            history_node_backends_set = group_history.nodes[-1]
        else:
            history_node_backends_set = GroupNodeBackendsSetRecord(set=GroupNodeBackendsSet())

        # extended node backends set which includes newly seen nodes,
        # do not discard lost nodes
        unaccounted_history_node_backends_set = GroupNodeBackendsSet(
            nb
            for nb in history_node_backends_set.set
            if nb not in current_state_node_backends_set
        )
        ext_current_state_node_backends_set = GroupNodeBackendsSetRecord(
            set=current_state_node_backends_set + unaccounted_history_node_backends_set
        )

        if ext_current_state_node_backends_set != history_node_backends_set:
            logger.info(
                'Group {} info does not match, last state: {}, '
                'current state: {}'.format(
                    group.group_id, history_node_backends_set, ext_current_state_node_backends_set
                )
            )
            return ext_current_state_node_backends_set

        return None

    def _new_group_couple_record(self, group, group_history):
        """
        Create new couple record for group history

        New record is not allowed to add "no couple" record to history.
        If group is new and uncoupled such record should be provided by uncoupled group
        init script (not implemented at the time).
        In case couple is being broken "no couple" record of non-automatic type creation
        should be provided by the action-performing code.

        The workflow is following:
            1) if group's current couple differs from the one set in the most recent
            history record create a new couple record with current group's couple;
            2) if group's current couple is <None>, new record should not be created.

        Returns:
            - new couple record to save;
            - <None> if couple record should not be updated.
        """

        if group.couple is None:
            return None

        storage_couple = GroupCoupleRecord(couple=group.couple.as_tuple())

        history_couple = (
            group_history.couples and group_history.couples[-1] or
            GroupCoupleRecord(couple=())
        )
        if history_couple != storage_couple:
            logger.info(
                'Group {} couple does not match, last state: {}, '
                'current state: {}'.format(
                    group.group_id, history_couple, storage_couple
                )
            )
            return storage_couple

        return None

    def _update_state(self):
        """
        Check if group's current state corresponds to group history record,
        update record if necessary
        """
        start_ts = time.time()

        failed_groups = set()

        def update_group_history_record(group, group_history):
            new_node_backends_set_record = self._new_group_node_backends_set_record(
                group=group,
                group_history=group_history,
            )

            new_couple_record = self._new_group_couple_record(
                group=group,
                group_history=group_history,
            )

            if new_node_backends_set_record or new_couple_record:
                self._update_group(
                    group_history=group_history,
                    new_nodes=new_node_backends_set_record,
                    new_couple=new_couple_record
                )

        try:
            logger.info('Updating infrastructure state')

            with self._groups_to_update_lock:
                groups_to_update = self._groups_to_update
                self._groups_to_update = set()

            group_ids = list(g.group_id for g in groups_to_update)
            group_histories = iter(self.group_history_finder.search_by_group_ids(group_ids))

            while groups_to_update:

                try:
                    group_history = group_histories.next()
                    group = storage.groups[group_history.group_id]
                    groups_to_update.remove(group)
                except StopIteration:
                    # executed only once per new group in storage
                    # so it does not introduce noticeable overhead
                    group = groups_to_update.pop()
                    group_history = self._new_group_history(group.group_id)

                try:
                    update_group_history_record(group, group_history)
                except CacheUpstreamError as e:
                    logger.error('Failed to update history for group {}: {}'.format(
                        group, e
                    ))
                    failed_groups.add(group)
                except Exception:
                    logger.exception('Failed to update history for group {}'.format(
                        group
                    ))
                    failed_groups.add(group)

        except Exception:
            logger.exception('Failed to update infrastructure state')
        finally:
            if failed_groups:
                logger.error('Failed to update history for {} groups'.format(len(failed_groups)))
                with self._groups_to_update_lock:
                    self._groups_to_update.update(failed_groups)

            logger.info('Finished updating infrastructure state, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _update_group(self,
                      group_history,
                      new_nodes=None,
                      new_couple=None,
                      record_type=GroupStateRecord.HISTORY_RECORD_AUTOMATIC):

        if new_nodes is not None:
            new_nodes.timestamp = time.time()
            new_nodes.type = record_type

            group_history.nodes.append(new_nodes)
            group_history._dirty = True

        if new_couple is not None:
            new_couple.timestamp = time.time()
            new_couple.type = record_type

            group_history.couples.append(new_couple)
            group_history._dirty = True

        group_history.save()

    # TODO: make family non-optional
    def detach_node(self, group_id, hostname, port, backend_id, family=None, record_type=None):
        group_history = self.get_group_history(group_id)

        node_backends_set = group_history.nodes[-1].set[:]

        for i, node_backend in enumerate(node_backends_set):
            backend_match = (
                node_backend.hostname == hostname and
                node_backend.port == port and
                node_backend.backend_id == backend_id
            )
            if backend_match:

                if family and family != node_backend.family:
                    # TODO: move family check to 'backend_match' check
                    # when 'family' is made non-optional
                    continue

                logger.debug(
                    'Removing node backend {0}:{1}/{2} from group {3} history state'.format(
                        hostname, port, backend_id, group_id
                    )
                )
                del node_backends_set[i]
                break
        else:
            raise ValueError(
                'Node backend {0}:{1}/{2} not found in group {3} history state'.format(
                    hostname, port, backend_id, group_id
                )
            )

        self._update_group(
            group_history=group_history,
            new_nodes=GroupNodeBackendsSetRecord(set=node_backends_set),
            record_type=record_type or GroupStateRecord.HISTORY_RECORD_MANUAL
        )

    def uncouple_groups(self, group_ids, record_type=None):
        """
        Create new record "no couple" for groups histories.
        """

        if not self.group_history_finder:
            return

        for group_id in group_ids:
            try:
                group_history = self.get_group_history(group_id)
            except ValueError:
                # NOTE: Recently created group with no history - create one
                group_history = self._new_group_history(group_id)

            self._update_group(
                group_history=group_history,
                new_couple=GroupCoupleRecord(couple=()),
                record_type=record_type or GroupStateRecord.HISTORY_RECORD_MANUAL
            )

    def move_group_cmd(self,
                       src_host,
                       src_port=None,
                       src_family=2,
                       src_path=None,
                       dst_port=None,
                       dst_path=None,
                       user=None,
                       file_tpl='data*'):
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

    def ttl_cleanup_cmd(self,
                        remotes,
                        couple,
                        iter_group,
                        trace_id=None,
                        safe=False,
                        attempts=None,
                        wait_timeout=None,
                        batch_size=None,
                        nproc=None,
                        remove_all_older=None,
                        remove_permanent_older=None):

        TTL_CLEANUP_CNF = config.get('infrastructure', {}).get('ttl_cleanup', {})

        if remove_all_older:
            remove_type = '--remove-all-older {}'.format(remove_all_older)
        elif remove_permanent_older:
            remove_type = '--remove-permanent-older {}'.format(remove_permanent_older)
        else:
            remove_type = '--remove-expired'

        cmd = self.TTL_CLEANUP_CMD.format(
            groups=",".join(str(g.group_id) for g in couple.groups),
            iter_group=iter_group,
            attempts=(attempts or TTL_CLEANUP_CNF.get('attempts', 3)),
            wait_timeout=(wait_timeout or TTL_CLEANUP_CNF.get('wait_timeout', 20)),
            nproc=(nproc or TTL_CLEANUP_CNF.get('nproc', 10)),
            batch_size=(batch_size or TTL_CLEANUP_CNF.get('batch_size', 100)),
            trace_id=(trace_id or int(uuid.uuid4().hex[:16], 16)),
            log=TTL_CLEANUP_CNF.get('log', 'ttl_cleanup.log'),
            log_level="info",
            tmp_dir=TTL_CLEANUP_CNF.get(
                'tmp_dir',
                '/var/tmp/ttl_cleanup_{couple_id}'
            ).format(
                couple_id=couple,
            ),
            safe=('-S' if safe else ''),
            remotes=(' '.join('-r {}'.format(r) for r in remotes)),
            tskv='--tskv-context namespace={},couple_id={} --tskv-log {}'.format(
                couple.namespace, couple.groups[0].group_id, TTL_CLEANUP_CNF.get('tskv_log_file', 'syslog')
            ),
            remove_type=remove_type,
        )

        return cmd

    @h.concurrent_handler
    def start_node_cmd(self, request):

        host, port, family = request[:3]

        cmd = inventory.node_start_command(host, port, family)

        if cmd is None:
            raise RuntimeError('Node start command is not provided by inventory implementation')

        logger.info('Command for starting elliptics node {0}:{1} was requested: {2}'.format(
            host, port, cmd
        ))

        return cmd

    @h.concurrent_handler
    def shutdown_node_cmd(self, request):

        host, port = request[:2]

        node_addr = '{0}:{1}'.format(host, port)

        if node_addr not in storage.nodes:
            raise ValueError("Node {0} doesn't exist".format(node_addr))

        node = storage.nodes[node_addr]

        cmd = inventory.node_shutdown_command(node.host, node.port, node.family)
        logger.info('Command for shutting down elliptics node {0} was requested: {1}'.format(
            node_addr, cmd
        ))

        return cmd

    _enable_node_backend_cmd = staticmethod(dnet_client_backend_command('enable'))
    _disable_node_backend_cmd = staticmethod(dnet_client_backend_command('disable'))
    _make_readonly_node_backend_cmd = staticmethod(dnet_client_backend_command('make_readonly'))
    _make_writable_node_backend_cmd = staticmethod(dnet_client_backend_command('make_writable'))
    _remove_node_backend_cmd = staticmethod(dnet_client_backend_command('remove'))

    @h.concurrent_handler
    def enable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id)

        cmd = self._enable_node_backend_cmd(host, port, family, backend_id)

        if cmd is None:
            raise RuntimeError(
                'Node backend start command is not provided by inventory implementation'
            )

        logger.info('Command for starting elliptics node {0} was requested: {1}'.format(
            nb_addr, cmd
        ))

        return cmd

    @h.concurrent_handler
    def disable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

        if nb_addr not in storage.node_backends:
            raise ValueError("Node backend {0} doesn't exist".format(nb_addr))

        nb = storage.node_backends[nb_addr]

        cmd = self._disable_node_backend_cmd(
            nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)
        logger.info(
            'Command for shutting down elliptics node backend {0} was requested: {1}'.format(
                nb_addr, cmd
            )
        )

        return cmd

    def make_readonly_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

        if nb_addr not in storage.node_backends:
            raise ValueError("Node backend {0} doesn't exist".format(nb_addr))

        nb = storage.node_backends[nb_addr]

        cmd = self._make_readonly_node_backend_cmd(
            nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)
        logger.info(
            'Command for making elliptics node backend {0} read-only was requested: {1}'.format(
                nb_addr, cmd
            )
        )

        return cmd

    def make_writable_node_backend_cmd(self, request):

        host, port, family, backend_id = request[:4]

        nb_addr = '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

        if nb_addr not in storage.node_backends:
            raise ValueError("Node backend {0} doesn't exist".format(nb_addr))

        nb = storage.node_backends[nb_addr]

        cmd = self._make_writable_node_backend_cmd(
            nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)
        logger.info(
            'Command for making elliptics node backend {0} writable was requested: {1}'.format(
                nb_addr, cmd
            )
        )

        return cmd

    @h.concurrent_handler
    def reconfigure_node_cmd(self, request):

        host, port, family = request[:3]

        node_addr = '{0}:{1}'.format(host, port)

        if node_addr not in storage.nodes:
            raise ValueError("Node {0} doesn't exist".format(node_addr))

        cmd = self._reconfigure_node_cmd(host, port, family)

        logger.info('Command for reconfiguring elliptics node {0} was requested: {1}'.format(
            node_addr, cmd
        ))

        return cmd

    def _reconfigure_node_cmd(self, host, port, family):

        cmd = inventory.node_reconfigure(host, port, family)

        if cmd is None:
            raise RuntimeError(
                'Node reconfiguration command is not provided by inventory implementation'
            )
        return cmd

    @h.concurrent_handler
    def recover_group_cmd(self, request):

        try:
            group_id = int(request[0])
            if group_id not in storage.groups:
                raise ValueError
        except (ValueError, TypeError):
            raise ValueError('Group {0} is not found'.format(request[0]))

        cmd = self._recover_group_cmd(group_id)

        logger.info('Command for dc recovery for group {0} was requested: {1}'.format(
            group_id, cmd
        ))

        return cmd

    def _recover_group_cmd(self, group_id, json_stats=False, tmp_dir=None, trace_id=None):
        group = storage.groups[group_id]
        if not group.couple:
            raise ValueError('Group {0} is not coupled'.format(group_id))

        remotes = []
        for g in group.couple.groups:
            for nb in g.node_backends:
                remotes.append(self.REMOTE_TPL.format(
                    host=nb.node.host.addr,
                    port=nb.node.port,
                    family=nb.node.family,
                ))

        if not tmp_dir:
            tmp_dir = RECOVERY_DC_CNF.get(
                'tmp_dir',
                '/var/tmp/dnet_recovery_dc_{group_id}'
            ).format(
                group_id=group_id,
                group_base_path=group.node_backends[0].base_path,
            )

        cmd = self.DNET_RECOVERY_DC_CMD.format(
            remotes=' '.join(remotes),
            groups=','.join(str(g) for g in group.couple.groups),
            tmp_dir=tmp_dir,
            attempts=RECOVERY_DC_CNF.get('attempts', 1),
            batch=RECOVERY_DC_CNF.get('batch', 2000),
            log=RECOVERY_DC_CNF.get('log', 'dnet_recovery.log').format(group_id=group_id),
            log_level=RECOVERY_DC_CNF.get('log_level', 1),
            processes_num=len(group.couple.groups) - 1 or 1,
            json_stats='-s json' if json_stats else '',
            trace_id=trace_id or uuid.uuid4().hex[:16],
        )

        return cmd

    @h.concurrent_handler
    def defrag_node_backend_cmd(self, request):

        try:
            host, port, family, backend_id = request[:4]
        except ValueError:
            raise ValueError(
                'Request should contain parameters in the following order: '
                'host, port, family, backend_id'
            )

        node_backend_str = '{0}:{1}/{2}'.format(host, port, backend_id)
        node_backend = storage.node_backends[node_backend_str]

        cmd = self._defrag_node_backend_cmd(
            node_backend.node.host.addr,
            node_backend.node.port,
            node_backend.node.family,
            node_backend.backend_id,
        )

        logger.info('Command for node backend {0} defragmentation was requested: {1}'.format(
            node_backend, cmd
        ))

        return cmd

    def _defrag_node_backend_cmd(self, host, port, family, backend_id):
        cmd = self.DNET_DEFRAG_CMD.format(
            host=host, port=port, family=family, backend_id=backend_id
        )
        return cmd

    def _lrc_convert_cmd(self,
                         couple,
                         src_groups,
                         dst_groups,
                         part_size,
                         scheme,
                         trace_id=None):

        remotes = []
        for g in src_groups + dst_groups:
            for nb in g.node_backends:
                remotes.append(
                    self.REMOTE_TPL.format(
                        host=nb.node.host.addr,
                        port=nb.node.port,
                        family=nb.node.family,
                    )
                )

        cmd = self.LRC_CONVERT_CMD.format(
            remotes=' '.join(set(remotes)),
            src_groups=','.join(str(g.group_id) for g in src_groups),
            dst_groups=','.join(str(g.group_id) for g in dst_groups),
            part_size=part_size,
            scheme=scheme,
            tmp_dir=LRC_CONVERT_DC_CNF.get(
                'tmp_dir',
                '/var/tmp/lrc_convert_{couple_id}'
            ).format(couple_id=couple),
            attempts=LRC_CONVERT_DC_CNF.get('attempts', 1),
            log=LRC_CONVERT_DC_CNF.get('log', 'lrc_convert.log').format(couple_id=couple),
            log_level=LRC_CONVERT_DC_CNF.get('log_level', 1),
            trace_id=trace_id or uuid.uuid4().hex[:16],
            data_flow_rate=LRC_CONVERT_DC_CNF.get('data_flow_rate', 10),  # MB/s
            wait_timeout=LRC_CONVERT_DC_CNF.get('wait_timeout', 20),  # seconds
        )

        return cmd

    def _lrc_validate_cmd(self,
                          couple,
                          src_groups,
                          dst_groups,
                          part_size,
                          scheme,
                          trace_id=None):

        remotes = []
        for g in src_groups + dst_groups:
            for nb in g.node_backends:
                remotes.append(
                    self.REMOTE_TPL.format(
                        host=nb.node.host.addr,
                        port=nb.node.port,
                        family=nb.node.family,
                    )
                )

        cmd = self.LRC_VALIDATE_CMD.format(
            remotes=' '.join(set(remotes)),
            src_groups=','.join(str(g.group_id) for g in src_groups),
            dst_groups=','.join(str(g.group_id) for g in dst_groups),
            part_size=part_size,
            scheme=scheme,
            tmp_dir=LRC_VALIDATE_DC_CNF.get(
                'tmp_dir',
                '/var/tmp/lrc_validate_{couple_id}'
            ).format(couple_id=couple),
            attempts=LRC_VALIDATE_DC_CNF.get('attempts', 1),
            log=LRC_VALIDATE_DC_CNF.get('log', 'lrc_validate.log').format(couple_id=couple),
            log_level=LRC_VALIDATE_DC_CNF.get('log_level', 1),
            trace_id=trace_id or uuid.uuid4().hex[:16],
            data_flow_rate=LRC_CONVERT_DC_CNF.get('data_flow_rate', 10),  # MB/s
            wait_timeout=LRC_CONVERT_DC_CNF.get('wait_timeout', 20),  # seconds
        )

        return cmd

    @h.concurrent_handler
    def search_history_by_path(self, request):
        params = request[0]

        try:
            host = params['host']
            path = params['path']
        except KeyError:
            raise ValueError('Host and path parameters are required')

        if not self.group_history_finder:
            return []

        entries = []

        start_idx = 0
        if params.get('last', False):
            start_idx = -1

        group_histories = self.group_history_finder.search_by_node_backend(
            hostname=host,
            path=path
        )
        path = self.group_history_finder.node_backend_path_to_regexp(path)
        path_re = re.compile(path)
        for group_history in group_histories:
            for node_backends_set in group_history.nodes[start_idx:]:
                for node_backend in node_backends_set.set:

                    if not node_backend.path:
                        continue

                    if path_re.match(node_backend.path) is not None and node_backend.hostname == host:
                        entries.append((group_history, node_backends_set))
                        break

        entries.sort(key=lambda e: e[0])
        result = []

        for group_history, node_backends_set in entries:
            result.append({'group': group_history.group_id,
                           'set': [nb.dump() for nb in node_backends_set.set],
                           'timestamp': node_backends_set.timestamp,
                           'type': node_backends_set.type})

        return result

    def cluster_tree(self, namespace=None):
        nodes = {}
        root = {}
        hosts = []

        if namespace:
            if namespace in storage.namespaces:
                for couple in storage.namespaces[namespace].couples:
                    hosts.extend(nb.node.host for g in couple for nb in g.node_backends)
            else:
                hosts = []
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

                if 'parent' not in tree_node:
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
            if k not in types:
                del nodes[k]

        nodes['hdd'] = {}

        for nb in storage.node_backends:

            try:
                full_path = nb.node.host.full_path
            except CacheUpstreamError:
                logger.warn('Skipping {} because of cache failure'.format(
                    nb.node.host))
                continue

            if full_path not in nodes['host']:
                logger.warn('Host {0} is not found in cluster tree'.format(full_path))
                continue
            if nb.stat is None:
                continue

            fsid = str(nb.stat.fsid)
            fsid_full_path = full_path + '|' + fsid
            if fsid_full_path not in nodes['hdd']:
                hdd_node = {
                    'type': 'hdd',
                    'name': fsid,
                    'full_path': fsid_full_path,
                }
                nodes['hdd'][fsid_full_path] = hdd_node
                nodes['host'][full_path].setdefault('children', []).append(hdd_node)

        return tree, nodes

    def update_groups_list(self, root):
        if 'children' not in root:
            return root.setdefault('groups', set())
        root['groups'] = reduce(
            operator.or_,
            (self.update_groups_list(child) for child in root.get('children', [])),
            set()
        )
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
                if hdd_path not in nodes['hdd']:
                    # when building cluster tree every host resolve operation
                    # failed for this hdd
                    continue
                nodes['hdd'][hdd_path].setdefault('groups', set()).add(group.group_id)

    def account_ns_couples(self, tree, nodes, namespace):

        for hdd in nodes['hdd'].itervalues():
            hdd['groups'] = set()

        if namespace in storage.namespaces:
            for couple in storage.namespaces[namespace].couples:

                if couple.status != storage.Status.OK:
                    continue

                self.account_ns_groups(nodes, couple.groups)

        self.update_groups_list(tree)

    def groups_by_total_space(self, match_group_space=True, group_total_space=None, **kwargs):
        """ Get good uncoupled groups bucketed by total space values

        As total space for group can vary a little depending on the hardware,
        there is a small tolerance value (5% by default) that is used when
        comparing two groups with each other. If total space of the smaller
        group lies within tolerance interval (e.g., -5%, +5%) of total space
        of the larger group, it will be considered to have the same total space.

        Parameters:
            match_group_space: if False, all groups in the resulting mapping will
                be bucketed to a key 'any', otherwise tolerance interval will be used
                to bucket the groups;
            group_total_space: groups that are used for couples should have total
                space within 'total_space_diff_tolerance' percents within required
                'group_total_space' (in bytes);
            **kwargs: options for selecting uncoupled groups, will be passed through
                to get_good_uncoupled_groups;

        Returns: mapping of approximate total space value of groups to a groups list
           having that value; if match_group_space is False, all groups will be listed
           under key 'any'.

        Examples:

            when match_group_space == True:
            {
                1073741824: [10, 20, 30],  # 1Gb groups
                5368709120: [15, 25],      # 5Gb groups
            }

            when match_group_space == False:
            {
                'any': [10, 15, 20, 25, 30],
            }
        """
        suitable_groups = self.get_good_uncoupled_groups(**kwargs)
        groups_by_total_space = {}

        if len(suitable_groups) == 0:
            logger.warn("No suitable uncoupled groups found")

        if not match_group_space:
            groups_by_total_space['any'] = [group.group_id for group in suitable_groups]
            return groups_by_total_space

        ts_tolerance = config.get('total_space_diff_tolerance', 0.05)

        if group_total_space:
            groups_by_total_space[group_total_space] = []
            for group in suitable_groups:
                ts = group.get_stat().total_space
                if abs(group_total_space - ts) < group_total_space * ts_tolerance:
                    groups_by_total_space[group_total_space].append(group.group_id)
        else:
            total_spaces = (
                group.get_stat().total_space
                for group in suitable_groups
            )
            # bucketing groups by approximate total space
            cur_ts_key = 0
            for ts in sorted(total_spaces, reverse=True):
                if abs(cur_ts_key - ts) > cur_ts_key * ts_tolerance:
                    cur_ts_key = ts
                    groups_by_total_space[cur_ts_key] = []

            total_spaces = sorted(groups_by_total_space.keys(), reverse=True)
            logger.info('group total space sizes available: {0}'.format(list(total_spaces)))

            for group in suitable_groups:
                ts = group.get_stat().total_space
                for ts_key in total_spaces:
                    if ts_key - ts < ts_key * ts_tolerance:
                        groups_by_total_space[ts_key].append(group.group_id)
                        break
                else:
                    raise ValueError(
                        'Failed to find total space key for group {group}, '
                        'total space {ts}'.format(
                            group=group,
                            ts=ts,
                        )
                    )

        return groups_by_total_space

    def ns_current_state(self, nodes, types):
        ns_current_state = {}
        for node_type in types:
            ns_current_state[node_type] = {'nodes': {},
                                           'avg': 0}
            for child in nodes[node_type].itervalues():
                if 'groups' not in child:
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
                        nb_units[parent['type']] = '|'.join(parts)
                    parts.pop()
                    parent = parent.get('parent')

                nb_units['hdd'] = nb_units['host'] + '|' + str(nb.stat.fsid)

                units[group.group_id].append(nb_units)

        return units

    def get_group_ids_in_service(self):
        group_ids_in_service = []
        if not self.job_finder:
            return group_ids_in_service
        for job in self.job_finder.jobs(statuses=jobs.Job.ACTIVE_STATUSES, sort=False):
            group_ids_in_service.extend(job._involved_groups)
        return group_ids_in_service

    def get_good_uncoupled_groups(self,
                                  max_node_backends=None,
                                  exclude_in_service=True,
                                  status=None,
                                  types=None,
                                  skip_groups=None,
                                  allow_alive_keys=False):

        if exclude_in_service and self.job_finder:
            in_service_group_ids = set(self.job_finder.get_uncoupled_groups_in_service())
        else:
            in_service_group_ids = None

        selector = UncoupledGroupsSelector(
            max_node_backends=max_node_backends,
            status=status or storage.Status.INIT,
            types=types,
            skip_groups=skip_groups,
            allow_alive_keys=allow_alive_keys,
            locked_hosts=manual_locker.get_locked_hosts(),
            in_service_group_ids=in_service_group_ids,
        )
        suitable_groups = selector.select()
        logger.debug('Uncoupled groups selector result: {}'.format(selector))
        return suitable_groups

    @staticmethod
    def is_uncoupled_group_good(group,
                                locked_hosts,
                                types,
                                max_node_backends=None,
                                in_service=None,
                                status=None,
                                allow_alive_keys=False):

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
            if not allow_alive_keys and nb.stat and nb.stat.files > 0:
                return False

        if group.type not in types:
            return False

        if max_node_backends and len(group.node_backends) > max_node_backends:
            return False

        return True

    def reserve_group_ids(self, count):
        logger.info('Storage max group: reserving {} groups'.format(count))
        result = max_group_manager.reserve_group_ids(count)
        logger.info('Storage max group: reserved groups: {}'.format(result))
        return result


class UncoupledGroupsSelector(object):

    def __init__(self,
                 groups=None,
                 max_node_backends=None,
                 status=None,
                 types=None,
                 skip_groups=None,
                 allow_alive_keys=False,
                 locked_hosts=None,
                 in_service_group_ids=None):

        self.groups = groups

        self.max_node_backends = max_node_backends
        self.status = status or storage.Status.INIT
        self.types = types or (storage.Group.TYPE_UNCOUPLED,)
        self.skip_groups = skip_groups
        self.allow_alive_keys = allow_alive_keys
        self.locked_hosts = locked_hosts
        self.in_service_group_ids = in_service_group_ids

        self._reset()

    def _reset(self):
        self._in_service_groups = []
        self._skipped_groups = []
        self._no_backends_groups = []
        self._mismatched_status_groups = []
        self._not_ok_backends_groups = []
        self._locked_hosts_groups = []
        self._alive_keys_groups = []
        self._mismatched_type_groups = []
        self._max_backends_groups = []
        self._unknown_dc_groups = []

        self._good_uncoupled_groups = []

    def select(self):
        self._reset()

        groups = self.groups or storage.groups.keys()

        for group in groups:
            self._dispatch_group(group)

        return self._good_uncoupled_groups

    def _dispatch_group(self, group):
        if group.couple is not None:
            # TODO: coupled groups are not saved since its the obvious reason
            return

        if self.in_service_group_ids is not None and group.group_id in self.in_service_group_ids:
            self._in_service_groups.append(group)
            return

        if self.skip_groups and group.group_id in self.skip_groups:
            self._skipped_groups.append(group)
            return

        if not len(group.node_backends):
            self._no_backends_groups.append(group)
            return

        if group.type not in self.types:
            self._mismatched_type_groups.append((group, group.type))
            return

        if group.status != self.status:
            self._mismatched_status_groups.append((group, group.status))
            return

        for nb in group.node_backends:
            if nb.status != storage.Status.OK:
                self._not_ok_backends_groups.append((group, nb.status))
                return

            if self.locked_hosts and nb.node.host in self.locked_hosts:
                self._locked_hosts_groups.append(group)
                return

            if not self.allow_alive_keys and nb.stat and nb.stat.files > 0:
                self._alive_keys_groups.append(group)
                return

            try:
                node_backend_dc = nb.node.host.dc
            except CacheUpstreamError:
                self._unknown_dc_groups.append(group)
                return

        if self.max_node_backends and len(group.node_backends) > self.max_node_backends:
            self._max_backends_groups.append((group, len(group.node_backends)))
            return

        self._good_uncoupled_groups.append(group)

    def __str__(self):
        s = 'selected uncoupled groups: {}'.format([g.group_id for g in self._good_uncoupled_groups])
        if self.in_service_group_ids is not None:
            s += '; groups in service: {}'.format([g.group_id for g in self._in_service_groups])
        if self.skip_groups:
            s += '; groups manually skipped: {}'.format([g.group_id for g in self._skipped_groups])
        s += '; groups with no backends: {}'.format([g.group_id for g in self._no_backends_groups])
        s += '; groups with mismatched status: {}'.format(
            [
                (g.group_id, status)
                for g, status in self._mismatched_status_groups
            ]
        )
        s += '; groups with not OK backends: {}'.format(
            [
                (g.group_id, status)
                for g, status in self._not_ok_backends_groups
            ]
        )
        s += '; groups on locked hosts: {}'.format([g.group_id for g in self._locked_hosts_groups])
        if not self.allow_alive_keys:
            s += '; groups with alive keys: {}'.format([g.group_id for g in self._alive_keys_groups])

        s += '; groups with mismatched type: {}'.format(
            [
                (g.group_id, type)
                for g, type in self._mismatched_type_groups
            ]
        )
        if self.max_node_backends:
            s += '; groups exceeding max backends limit: {}'.format(
                [
                    (g.group_id, count)
                    for g, count in self._max_backends_groups
                ]
            )
        s += '; groups with unknown dcs: {}'.format([g.group_id for g in self._unknown_dc_groups])
        return s


infrastructure = Infrastructure()
