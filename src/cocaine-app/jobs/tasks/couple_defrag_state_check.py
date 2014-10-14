import logging
import time

from jobs import JobBrokenError, TaskTypes
import storage
from task import Task


logger = logging.getLogger('mm.jobs')


class CoupleDefragStateCheckTask(Task):

    PARAMS = ('couple', 'stats_ts')
    TASK_TIMEOUT = 60 * 60 * 24  # 1 day

    def __init__(self, job):
        super(CoupleDefragStateCheckTask, self).__init__(job)
        self.type = TaskTypes.TYPE_COUPLE_DEFRAG_STATE_CHECK_TASK

    def update_status(self):
        # infrastructure state is updated by itself via task queue
        pass

    def execute(self):
        couple = storage.couples[self.couple]

        if couple.status not in storage.GOOD_STATUSES:
            raise JobBrokenError('Couple {0} has inappropriate status: {1}'.format(self.couple, couple.status))

        stats = []
        for group in couple.groups:
            for nb in group.node_backends:
                stats.append(nb.stat)
        self.stats_ts = max([s.ts for s in stats])

    @property
    def finished(self):
        return (self.__couple_defraged() or
                time.time() - self.start_ts > self.TASK_TIMEOUT)

    @property
    def failed(self):
        return (time.time() - self.start_ts > self.TASK_TIMEOUT and
                not self.__couple_defraged())

    def __couple_defraged(self):
        couple = storage.couples[self.couple]
        stats = []
        for group in couple.groups:
            for nb in group.node_backends:
                stats.append(nb.stat)
        cur_stats_ts = min([s.ts for s in stats])
        if cur_stats_ts <= self.stats_ts:
            logger.info('Job {0}, task {1}: defrag status not updated since {2}'.format(
                self.parent_job.id, self.id, self.stats_ts))
            return False

        if all([s.defrag_state == 0 for s in stats]):
            logger.debug('Job {0}, task {1}: defrag finished, start_ts {2}, current ts {3}, '
                'defrag statuses {4}'.format(self.parent_job.id, self.id, self.stats_ts,
                    cur_stats_ts, [s.defrag_state for s in stats]))
            return True

        logger.info('Job {0}, task {1}: defrag not finished, defrag statuses {2}'.format(
            self.parent_job.id, self.id, [s.defrag_state for s in stats]))
        return False

    def __str__(self):
        return 'CoupleDefragStateCheckTask[id: {0}]<defrag of couple {1}>'.format(
            self.id, self.couple)