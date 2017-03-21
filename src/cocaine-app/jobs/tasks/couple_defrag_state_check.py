import logging
import time

from jobs import JobBrokenError, TaskTypes
import storage
from task import Task


logger = logging.getLogger('mm.jobs')


class CoupleDefragStateCheckTask(Task):

    PARAMS = ('couple', 'stats_ts')
    TASK_TIMEOUT = 60 * 60 * 24 * 14  # 14 days

    def __init__(self, job):
        super(CoupleDefragStateCheckTask, self).__init__(job)
        self.type = TaskTypes.TYPE_COUPLE_DEFRAG_STATE_CHECK_TASK

    def _update_status(self, processor):
        # infrastructure state is updated by itself via task queue
        pass

    def _terminate(self, processor):
        # cannot terminate task, since this task works only synchronously
        # early cleanup phase breaks nothing
        pass

    def _execute(self, processor):
        # TODO: use 'couples' container
        couples = (storage.cache_couples
                   if self.parent_job.is_cache_couple else
                   storage.replicas_groupsets)

        couple = couples[self.couple]

        stats = []
        for group in couple.groups:
            for nb in group.node_backends:
                if not nb.stat:
                    continue
                stats.append(nb.stat)
        stats_ts = [int(time.time())]
        if stats:
            stats_ts.extend(s.ts for s in stats)
        self.stats_ts = max(stats_ts)

    def finished(self, processor):
        return (self.__couple_defraged() or
                time.time() - self.start_ts > self.TASK_TIMEOUT)

    def failed(self, processor):
        return (time.time() - self.start_ts > self.TASK_TIMEOUT and
                not self.__couple_defraged())

    def __couple_defraged(self):
        # TODO: use 'couples' container
        couples = (storage.cache_couples
                   if self.parent_job.is_cache_couple else
                   storage.replicas_groupsets)
        couple = couples[self.couple]
        stats = []
        for group in couple.groups:
            if not group.node_backends:
                return False
            for nb in group.node_backends:
                if not nb.stat:
                    return False
                stats.append(nb.stat)
        cur_stats_ts = min(s.ts for s in stats)
        if cur_stats_ts <= self.stats_ts:
            logger.info('Job {0}, task {1}: defrag status not updated since {2}'.format(
                self.parent_job.id, self.id, self.stats_ts))
            return False

        if all(s.defrag_state == 0 for s in stats):
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
