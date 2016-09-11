import tasks
from job import Job
from job_types import JobTypes
import logging
import storage
from infrastructure import infrastructure

logger = logging.getLogger('mm.jobs')


class TtlCleanupJob(Job):

    PARAMS = (
        'iter_group',
        'batch_size',
        'attempts',
        'nproc',
        'wait_timeout',
        'dry_run',
        'resources'
    )

    def __init__(self, **kwargs):
        super(TtlCleanupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_TTL_CLEANUP_JOB

    def _set_resources(self):
        self.resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
        }

        # Addressing self.iter_group is more or less safe here since
        # set resources is raised soon after initialization
        # and on initialization iter_group is validated

        # The nodes that are not iterated would be asked to perform remove
        # operation. No data is written. And no data is read.
        # While iteration group node is working heavily
        nb = storage.groups[self.iter_group].node_backends[0]
        self.resources[Job.RESOURCE_HOST_IN].append(nb.node.host.addr)
        self.resources[Job.RESOURCE_FS].append((nb.node.host.addr, str(nb.fs.fsid)))

    def create_tasks(self, processor):

        # Addressing by iter_group may cause an exception of mm was restared
        # or group disappeared. But it is better to handle exceptions
        # and manually restart the job then simply ignore undone work
        iter_group_desc = storage.groups[self.iter_group]
        couple = iter_group_desc.couple
        groups = []
        remotes = []
        for g in couple.groups:
            groups.append(g.group_id)
            node = g.node_backends[0].node
            remotes.append("{}:{}:{}".format(node.host.addr, node.port, node.family))

        # Log, Log_level, temp to be taken from config on infrastructure side
        ttl_cleanup_cmd = infrastructure.ttl_cleanup_cmd(
            remotes=remotes,
            groups=groups,
            iter_group=self.iter_group,
            wait_timeout=self.wait_timeout,
            batch_size=self.batch_size,
            attempts=self.attempts,
            nproc=self.nproc,
            trace_id=int(self.id[:16], 16),
            safe=self.dry_run)

        logger.debug("TTl cleanup job: Set for execution task %s", ttl_cleanup_cmd)

        # Run langolier on the storage node where we are going to iterate
        nb = iter_group_desc.node_backends[0]
        host = nb.node.host.addr
        task = tasks.MinionCmdTask.new(
            self,
            host=host,
            group=self.iter_group,
            cmd=ttl_cleanup_cmd,
            params={},
        )
        self.tasks.append(task)

    @property
    def _involved_groups(self):
        # Addressing by iter_group may cause an exception of mm was restared
        # or group disappeared. But it is better to handle exceptions
        # and manually restart the job then simply ignore undone work
        couple = storage.groups[self.iter_group].couple
        return [g.group_id for g in couple.groups]

    @property
    def _involved_couples(self):
        # Addressing by iter_group may cause an exception. See comment in _involved_groups
        return [str(storage.groups[self.iter_group].couple)]
