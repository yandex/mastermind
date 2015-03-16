import logging
import time

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import NodeBackendDefragTask, CoupleDefragStateCheckTask, RecoverGroupDcTask
import storage
from sync import sync_manager
from sync.error import (
    LockError,
    LockFailedError,
    LockAlreadyAcquiredError,
    InconsistentLockError,
    API_ERROR_CODE
)


logger = logging.getLogger('mm.jobs')


class RecoverDcJob(Job):

    PARAMS = ('group', 'couple',
              'keys', 'host', 'port', 'family', 'backend_id' # read-only parameters
             )

    def __init__(self, **kwargs):
        super(RecoverDcJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RECOVER_DC_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(RecoverDcJob, cls).new(*args, **kwargs)
        try:
            couple = storage.couples[kwargs['couple']]
            keys = []
            for g in couple.groups:
                if not g.node_backends:
                    raise JobBrokenError('Group {0} has no active backends, '
                        'cannot create recover job'.format(g.group_id))
                keys.append(g.get_stat().files)
            keys.sort(reverse=True)
            job.keys = keys

            min_keys_group = job.__min_keys_group(couple)
            nb = min_keys_group.node_backends[0]
            job.group = min_keys_group.group_id
            job.host = nb.node.host.addr
            job.port = nb.node.port
            job.backend_id = nb.backend_id
            job.family = nb.node.family
        except Exception:
            job.release_locks()
            raise
        return job

    def human_dump(self):
        data = super(RecoverDcJob, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    def __min_keys_group(self, couple):
        return sorted(couple.groups, key=lambda g: g.get_stat().files)[0]

    def create_tasks(self):

        if not self.couple in storage.couples:
            raise JobBrokenError('Couple {0} is not found'.format(self.couple))

        couple = storage.couples[self.couple]

        recover_cmd = infrastructure._recover_group_cmd(self.group)
        task = RecoverGroupDcTask.new(self,
            group=self.group,
            host=self.host,
            cmd=recover_cmd,
            params={'node_backend': self.node_backend(
                        self.host, self.port, self.backend_id).encode('utf-8'),
                    'group': str(self.group)})
        self.tasks.append(task)

    def on_complete(self, processor):
        processor.planner.update_recover_ts(self.couple, time.time())

    @property
    def _involved_groups(self):
        if self.couple is None:
            # fallback to old recover dc job format
            group = storage.groups[self.group]
        else:
            # get couple from group, because couple id could have been altered
            # (sad but true)
            group_id = int(self.couple.split(':')[0])
            group = storage.groups[group_id]
        couple = group.couple
        if self.couple != str(couple):
            self.couple = str(couple)

        group_ids = [g.group_id for g in couple.groups]

        return group_ids

    @property
    def _involved_couples(self):
        if self.couple is None:
            # fallback to old recover dc job format
            group = storage.groups[self.group]
        else:
            group_id = int(self.couple.split(':')[0])
            group = storage.groups[group_id]
        couple = group.couple

        return [str(couple)]
