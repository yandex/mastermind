import logging
import math

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import Task, NodeBackendDefragTask, CoupleDefragStateCheckTask
import storage


logger = logging.getLogger('mm.jobs')


class CoupleDefragJob(Job):

    PARAMS = ('couple', 'fragmentation', 'is_cache_couple', 'resources')

    def __init__(self, **kwargs):
        super(CoupleDefragJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_COUPLE_DEFRAG_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(CoupleDefragJob, cls).new(*args, **kwargs)
        try:
            # TODO: use 'couples' container
            couples = (storage.cache_couples
                       if kwargs.get('is_cache_couple', False) else
                       storage.replicas_groupsets)
            couple = couples[kwargs['couple']]
            fragmentation = []
            for g in couple.groups:
                fragmentation.append(g.get_stat().fragmentation)
            fragmentation.sort(reverse=True)
            job.fragmentation = fragmentation
        except Exception:
            job.release_locks()
            raise
        return job

    def _set_resources(self):
        resources = {
            Job.RESOURCE_FS: [],
            Job.RESOURCE_CPU: [],
        }

        # TODO: use 'couples' container
        couples = (storage.cache_couples
                   if self.is_cache_couple else
                   storage.replicas_groupsets)
        couple = couples[self.couple]

        for g in couple.groups:
            resources[Job.RESOURCE_FS].append(
                (g.node_backends[0].node.host.addr, str(g.node_backends[0].fs.fsid)))
            resources[Job.RESOURCE_CPU].append(g.node_backends[0].node.host.addr)
        self.resources = resources

    def create_tasks(self, processor):
        # TODO: use 'couples' container
        couples = (storage.cache_couples
                   if self.is_cache_couple else
                   storage.replicas_groupsets)
        if not self.couple in couples:
            raise JobBrokenError('Couple {0} is not found'.format(self.couple))

        couple = couples[self.couple]

        def make_defrag_tasks(nb):
            cmd = infrastructure._defrag_node_backend_cmd(
                nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)

            node_backend = self.node_backend(
                nb.node.host.addr, nb.node.port, nb.backend_id)

            task = NodeBackendDefragTask.new(self,
                host=nb.node.host.addr,
                cmd=cmd,
                node_backend=node_backend,
                group=group.group_id,
                params={'group': str(group.group_id),
                        'node_backend': node_backend.encode('utf-8'),
                        'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]})

            self.tasks.append(task)

        defrag_tasks = False
        for group in couple.groups:
            for nb in group.node_backends:
                if nb.stat.want_defrag <= 1:
                    continue
                make_defrag_tasks(nb)
                task = CoupleDefragStateCheckTask.new(self, couple=str(couple))
                self.tasks.append(task)
                defrag_tasks = True

        if not defrag_tasks:
            raise ValueError("Couple's {} backends does not require "
                             "defragmentation".format(self.couple))

    @property
    def group(self):
        group = self._involved_groups[0]
        for task in self.tasks:
            if task.type != 'node_backend_defrag_task':
                continue
            if task.status == Task.STATUS_QUEUED:
                break
            group = task.group
        return group

    @property
    def _involved_groups(self):
        return [int(gid) for gid in self.couple.split(':')]

    @property
    def _involved_couples(self):
        return [self.couple]
