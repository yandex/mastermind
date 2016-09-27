import os.path
import logging
import time

from error import JobBrokenError
import infrastructure
from infrastructure_cache import cache
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
              'resources',
              'keys', 'host', 'port', 'family', 'backend_id' # read-only parameters
             )

    def __init__(self, **kwargs):
        super(RecoverDcJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RECOVER_DC_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(RecoverDcJob, cls).new(*args, **kwargs)
        try:
            couple = storage.replicas_groupsets[kwargs['couple']]
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

    def _set_resources(self):
        resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
        }

        couple = storage.replicas_groupsets[self.couple]
        for g in couple.groups:
            resources[Job.RESOURCE_HOST_IN].append(g.node_backends[0].node.host.addr)
            resources[Job.RESOURCE_HOST_OUT].append(g.node_backends[0].node.host.addr)
            resources[Job.RESOURCE_FS].append((g.node_backends[0].node.host.addr, str(g.node_backends[0].fs.fsid)))
        self.resources = resources

    def human_dump(self):
        data = super(RecoverDcJob, self).human_dump()
        data['hostname'] = cache.get_hostname_by_addr(data['host'], strict=False)
        return data

    def __min_keys_group(self, couple):
        return sorted(couple.groups, key=lambda g: g.get_stat().files)[0]

    def create_tasks(self, processor):

        if not self.couple in storage.replicas_groupsets:
            raise JobBrokenError('Couple {0} is not found'.format(self.couple))

        couple = storage.replicas_groupsets[self.couple]

        group = storage.groups[self.group]

        tmp_dir = infrastructure.RECOVERY_DC_CNF.get(
            'tmp_dir',
            '/var/tmp/dnet_recovery_dc_{group_id}'
        ).format(
            group_id=group.group_id,
            group_base_path=group.node_backends[0].base_path,
        )

        recover_cmd = infrastructure.infrastructure._recover_group_cmd(
            self.group,
            json_stats=True,
            trace_id=self.id[:16],
        )

        # here we force dnet_recovery to dump output to json stats file,
        # its filename is hardcoded in dnet_recovery utility as stats.json
        commands_stats_path = os.path.join(tmp_dir, 'stats.json')

        task = RecoverGroupDcTask.new(
            self,
            group=self.group,
            host=self.host,
            cmd=recover_cmd,
            json_stats=True,
            tmp_dir=tmp_dir,
            params={
                'node_backend': self.node_backend(
                    host=self.host,
                    port=self.port,
                    backend_id=self.backend_id,
                ),
                'group': str(self.group),
                'commands_stats_path': commands_stats_path,
            }
        )
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
