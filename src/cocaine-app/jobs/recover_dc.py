import logging

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

    PARAMS = ('group', 'host', 'port', 'family', 'backend_id', 'keys')

    def __init__(self, **kwargs):
        super(RecoverDcJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RECOVER_DC_JOB

    @classmethod
    def new(cls, **kwargs):
        job = super(RecoverDcJob, cls).new(**kwargs)
        group = storage.groups[kwargs['group']]
        keys = []
        for g in group.couple.groups:
            keys.append(g.get_stat().files)
        keys.sort(reverse=True)
        job.keys = keys
        return job

    def human_dump(self):
        data = super(RecoverDcJob, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    def create_tasks(self):

        if not self.group in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(self.group))

        group = storage.groups[self.group]

        if not group.couple:
            raise JobBrokenError('Group {0} does not participate in any couple'.format(self.group))

        for group in group.couple.groups:
            for nb in group.node_backends:
                cmd = infrastructure.defrag_node_backend_cmd([
                    nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id])

                node_backend = self.node_backend(
                    nb.node.host.addr, nb.node.port, nb.backend_id)

                task = NodeBackendDefragTask.new(self,
                    host=nb.node.host.addr,
                    cmd=cmd,
                    node_backend=node_backend,
                    group=group.group_id,
                    params={'group': str(group.group_id),
                            'node_backend': node_backend.encode('utf-8')})

                self.tasks.append(task)

        task = CoupleDefragStateCheckTask.new(self,
                                              couple=str(group.couple))
        self.tasks.append(task)

        recover_cmd = infrastructure.recover_group_cmd([self.group])
        task = RecoverGroupDcTask.new(self,
            group=self.group,
            host=self.host,
            cmd=recover_cmd,
            params={'node_backend': self.node_backend(
                        self.host, self.port, self.backend_id).encode('utf-8'),
                    'group': str(self.group)})
        self.tasks.append(task)

    @property
    def _locks(self):
        if not self.group in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(self.group))

        group = storage.groups[self.group]

        if not group.couple:
            raise JobBrokenError('Group {0} does not participate in any couple'.format(self.group))

        return (['{0}{1}'.format(self.GROUP_LOCK_PREFIX, g.group_id) for g in group.couple.groups] +
                ['{0}{1}'.format(self.COUPLE_LOCK_PREFIX, str(group.couple))])
