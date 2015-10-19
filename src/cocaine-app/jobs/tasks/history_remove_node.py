import logging
import time

import history
from infrastructure import infrastructure
from infrastructure_cache import cache
from jobs import TaskTypes
import storage
from sync import sync_manager
from task import Task


logger = logging.getLogger('mm.jobs')


class HistoryRemoveNodeTask(Task):

    PARAMS = ('group', 'host', 'port', 'backend_id')
    TASK_TIMEOUT = 600

    def __init__(self, job):
        super(HistoryRemoveNodeTask, self).__init__(job)
        self.type = TaskTypes.TYPE_HISTORY_REMOVE_NODE

    def update_status(self):
        # infrastructure state is updated by itself via task queue
        pass

    def execute(self):
        nb_str = '{0}:{1}/{2}'.format(self.host, self.port, self.backend_id).encode('utf-8')
        try:
            logger.info('Job {0}, task {1}: removing node backend {2} '
                'from group {3} history'.format(
                    self.parent_job.id, self.id, nb_str, self.group))
            infrastructure.detach_node(self.group, self.host, self.port, self.backend_id,
                history.GroupStateRecord.HISTORY_RECORD_JOB)
            logger.info('Job {0}, task {1}: removed node backend {2} '
                'from group {3} history'.format(
                    self.parent_job.id, self.id, nb_str, self.group))
        except ValueError as e:
            # TODO: Think about changing ValueError to some dedicated exception
            # to differentiate between event when there is no such node in group
            # and an actual ValueError being raised
            logger.error('Job {0}, task {1}: failed to remove node backend {2} '
                'from group {3} history: {4}'.format(
                    self.parent_job.id, self.id, nb_str, self.group, e))
            pass

        group = self.group in storage.groups and storage.groups[self.group] or None
        node_backend = nb_str in storage.node_backends and storage.node_backends[nb_str] or None
        if group and node_backend and node_backend in group.node_backends:
            logger.info('Job {0}, task {1}: removing node backend {2} '
                'from group {3} node backends'.format(
                    self.parent_job.id, self.id, node_backend, group))
            group.remove_node_backend(node_backend)
            group.update_status_recursive()
            logger.info('Job {0}, task {1}: removed node backend {2} '
                'from group {3} node backends'.format(
                    self.parent_job.id, self.id, node_backend, group))

    def human_dump(self):
        data = super(HistoryRemoveNodeTask, self).human_dump()
        data['hostname'] = cache.get_hostname_by_addr(data['host'], strict=False)
        return data

    @property
    def finished(self):
        return (not self.__node_in_group() or
                time.time() - self.start_ts > self.TASK_TIMEOUT)

    @property
    def failed(self):
        return (time.time() - self.start_ts > self.TASK_TIMEOUT and
                self.__node_in_group())

    def __node_in_group(self):
        group = self.group in storage.groups and storage.groups[self.group] or None
        nb_str = '{0}:{1}/{2}'.format(self.host, self.port, self.backend_id).encode('utf-8')
        node_backend = nb_str in storage.node_backends and storage.node_backends[nb_str] or None

        if group and node_backend:
            logger.debug('Job {0}, task {1}: checking node backend {2} '
                'with group {3} node backends: {4}'.format(
                    self.parent_job.id, self.id, node_backend, self.group, group.node_backends))
            nb_in_group = node_backend.group is group
        else:
            nb_in_group = False

        nb_in_history = infrastructure.node_backend_in_last_history_state(
            self.group, self.host, self.port, self.backend_id)
        logger.debug('Job {0}, task {1}: checking node backend {2} '
            'in group {3} history set: {4}'.format(
                self.parent_job.id, self.id, nb_str, self.group, nb_in_history))

        if nb_in_group:
            logger.info('Job {0}, task {1}: node backend {2} is still '
                'in group {3}'.format(self.parent_job.id, self.id, nb_str, self.group))
        if nb_in_history:
            logger.info('Job {0}, task {1}: node backend {2} is still '
                'in group\'s {3} history'.format(
                    self.parent_job.id, self.id, nb_str, self.group))

        return nb_in_group or nb_in_history

    def __str__(self):
        return 'HistoryRemoveNodeTask[id: {0}]<remove {1}:{2}/{3} from group {4}>'.format(
            self.id, self.host, self.port, self.backend_id, self.group)
