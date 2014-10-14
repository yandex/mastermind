import logging

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import NodeBackendDefragTask
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


class CoupleDefragJob(Job):

    PARAMS = ('couple', 'fragmentation')

    def __init__(self, **kwargs):
        super(CoupleDefragJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_COUPLE_DEFRAG_JOB

    @classmethod
    def new(cls, **kwargs):
        job = super(CoupleDefragJob, cls).new(**kwargs)
        couple = storage.couples[kwargs['couple']]
        fragmentation = []
        for g in couple.groups:
            fragmentation.append(g.get_stat().fragmentation)
        fragmentation.sort(reverse=True)
        job.fragmentation = fragmentation
        return job

    def node_backend(self, host, port, backend_id):
        return '{0}:{1}/{2}'.format(host, port, backend_id).encode('utf-8')

    def human_dump(self):
        data = super(CoupleDefragJob, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    def create_tasks(self):
        if not self.couple in storage.couples:
            raise JobBrokenError('Couple {0} is not found'.format(self.couple))

        couple = storage.couples[self.couple]

        for group in couple.groups:
            for nb in group.node_backends:
                cmd = infrastructure.defrag_node_backend_cmd([
                    nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id])

                node_backend = self.node_backend(
                    nb.node.host.addr, nb.node.port, nb.backend_id)

                task = NodeBackendDefragTask.new(self,
                    host=self.nb.node.host.addr,
                    cmd=cmd,
                    node_backend=node_backend,
                    group=group.group_id,
                    params={'group': group.group_id,
                            'node_backend': node_backend})

                self.tasks.append(task)

    def perform_locks(self):
        try:
            sync_manager.persistent_locks_acquire(
                ['{0}{1}'.format(self.GROUP_LOCK_PREFIX, self.group)], self.id)
        except LockAlreadyAcquiredError as e:
            logger.error('Job {0}: group {1} is already '
                'being processed by job {2}'.format(self.id, self.group, e.holder_id))

            last_error = self.error_msg and self.error_msg[-1] or None
            if last_error and (last_error.get('code') != API_ERROR_CODE.LOCK_ALREADY_ACQUIRED or
                               last_error.get('holder_id') != e.holder_id):
                self.add_error(e)

            raise

    def release_locks(self):
        try:
            sync_manager.persistent_locks_release(
                ['{0}{1}'.format(self.GROUP_LOCK_PREFIX, self.group)], self.id)
        except InconsistentLockError as e:
            logger.error('Job {0}: lock for group {1} is already acquired by another '
                'job {2}'.format(self.id, self.group, e.holder_id))
            pass