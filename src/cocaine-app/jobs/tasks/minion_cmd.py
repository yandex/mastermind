import logging
import time

from infrastructure import infrastructure
from jobs import TaskTypes
from task import Task


logger = logging.getLogger('mm.jobs')


class MinionCmdTask(Task):

    PARAMS = ('group', 'host', 'cmd', 'params', 'minion_cmd_id')
    TASK_TIMEOUT = 6000

    def __init__(self, job):
        super(MinionCmdTask, self).__init__(job)
        self.minion_cmd = None
        self.minion_cmd_id = None
        self.type = TaskTypes.TYPE_MINION_CMD

    def update_status(self, processor):
        try:
            self.minion_cmd = processor.minions._get_command(self.minion_cmd_id)
            logger.debug('Job {0}, task {1}, minion command status was updated: {2}'.format(
                self.parent_job.id, self.id, self.minion_cmd))
        except ValueError:
            logger.warn('Job {0}, task {1}, minion command status {2} is not fetched '
                'from minions'.format(self.parent_job.id, self.id, self.minion_cmd_id))
            pass

    def execute(self, processor):
        minion_response = processor.minions._execute_cmd(self.host,
            self.cmd, self.params)
        self.minion_cmd = minion_response.values()[0]
        logger.info('Job {0}, task {1}, minions task execution: {2}'.format(
            self.parent_job.id, self.id, self.minion_cmd))
        self.minion_cmd_id = self.minion_cmd['uid']

    def human_dump(self):
        data = super(MinionCmdTask, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    @property
    def finished(self):
        return ((self.minion_cmd is None and
                 time.time() - self.start_ts > self.TASK_TIMEOUT) or
                (self.minion_cmd and self.minion_cmd['progress'] == 1.0))

    @property
    def failed(self):
        return self.minion_cmd is None or self.minion_cmd['exit_code'] != 0

    def __str__(self):
        return 'MinionCmdTask[id: {0}]<{1}>'.format(self.id, self.cmd)
