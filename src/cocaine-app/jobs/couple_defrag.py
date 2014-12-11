import logging

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import NodeBackendDefragTask, CoupleDefragStateCheckTask
import storage


logger = logging.getLogger('mm.jobs')


class CoupleDefragJob(Job):

    PARAMS = ('couple', 'fragmentation')

    def __init__(self, **kwargs):
        super(CoupleDefragJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_COUPLE_DEFRAG_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(CoupleDefragJob, cls).new(*args, **kwargs)
        try:
            couple = storage.couples[kwargs['couple']]
            fragmentation = []
            for g in couple.groups:
                fragmentation.append(g.get_stat().fragmentation)
            fragmentation.sort(reverse=True)
            job.fragmentation = fragmentation
        except Exception:
            job.release_locks()
            raise
        return job

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
                    host=nb.node.host.addr,
                    cmd=cmd,
                    node_backend=node_backend,
                    group=group.group_id,
                    params={'group': str(group.group_id),
                            'node_backend': node_backend.encode('utf-8')})

                self.tasks.append(task)

        task = CoupleDefragStateCheckTask.new(self, couple=str(couple))
        self.tasks.append(task)

    @property
    def _locks(self):
        group_ids = self.couple.split(':')
        return (['{0}{1}'.format(self.GROUP_LOCK_PREFIX, g) for g in group_ids] +
                ['{0}{1}'.format(self.COUPLE_LOCK_PREFIX, self.couple)])
