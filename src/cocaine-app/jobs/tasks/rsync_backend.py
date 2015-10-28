import logging

from errors import CacheUpstreamError
from infrastructure_cache import cache
import inventory
import jobs
from jobs import JobBrokenError, TaskTypes
from minion_cmd import MinionCmdTask
import storage


logger = logging.getLogger('mm.jobs')


class RsyncBackendTask(MinionCmdTask):

    PARAMS = MinionCmdTask.PARAMS + ('node_backend', 'src_host')

    def __init__(self, job):
        super(RsyncBackendTask, self).__init__(job)
        self.type = TaskTypes.TYPE_RSYNC_BACKEND_TASK

    def execute(self, processor):
        logger.info(
            'Job {job_id}, task {task_id}: checking group {group_id} '
            'and node backend {nb} state'.format(
                job_id=self.parent_job.id,
                task_id=self.id,
                group_id=self.group,
                nb=self.node_backend
            )
        )

        if self.node_backend:
            # Check if old backend is down or if a group is already running on a
            # different node backends
            # This check is not applied to move job
            current_group_node_backends = []
            if self.group in storage.groups:
                group = storage.groups[self.group]
                current_group_node_backends = set([nb for nb in group.node_backends])

            if self.node_backend in storage.node_backends:
                old_node_backend = storage.node_backends[self.node_backend]
                expected_statuses = (storage.Status.STALLED, storage.Status.INIT, storage.Status.RO)
                old_group_node_backend_is_up = (
                    old_node_backend in current_group_node_backends and
                    old_node_backend.status not in expected_statuses
                )
                if old_group_node_backend_is_up:
                    raise JobBrokenError(
                        'Node backend {nb} has status {status}, '
                        'expected {expected_statuses}'.format(
                            nb=old_node_backend,
                            status=old_node_backend.status,
                            expected_statuses=expected_statuses
                        )
                    )
                current_group_node_backends.discard(old_node_backend)

            if current_group_node_backends:
                raise JobBrokenError(
                    'Group {} is running on unexpected backends {}'.format(
                        self.group,
                        [str(nb) for nb in current_group_node_backends]
                    )
                )

        super(RsyncBackendTask, self).execute(processor)

    def __hostnames(self, hosts):
        hostnames = []
        for host in hosts:
            try:
                hostnames.append(cache.get_hostname_by_addr(host))
            except CacheUpstreamError:
                raise RuntimeError('Failed to resolve host {0}'.format(host))
        return hostnames

    def on_exec_start(self, processor):
        hostnames = set(self.__hostnames([self.host, self.src_host]))

        dl = jobs.Job.list(processor.downtimes,
                           host=list(hostnames), type='network_load')

        set_hostnames = set(record['host'] for record in dl)
        not_set_hostnames = hostnames - set_hostnames

        if not_set_hostnames:
            try:
                for hostname in not_set_hostnames:
                    inventory.set_net_monitoring_downtime(hostname)
            except Exception as e:
                logger.error(
                    'Job {job_id}, task {task_id}: failed to set net monitoring downtime: '
                    '{error}'.format(
                        job_id=self.parent_job.id,
                        task_id=self.id,
                        error=e
                    )
                )
                raise

        try:
            bulk_op = processor.downtimes.initialize_unordered_bulk_op()
            for hostname in hostnames:
                bulk_op.insert({'job_id': self.parent_job.id,
                                'host': hostname,
                                'type': 'network_load'})
            res = bulk_op.execute()
            if res['nInserted'] != len(hostnames):
                raise ValueError('failed to set all downtimes: {0}/{1}'.format(
                    res['nInserted'], len(hostnames)))
        except Exception as e:
            logger.error(
                'Job {job_id}, task {task_id}: unexpected mongo error: {error}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    error=e
                )
            )
            raise

    def on_exec_stop(self, processor):
        hostnames = set(self.__hostnames([self.host, self.src_host]))

        dl = jobs.Job.list(processor.downtimes,
                           host=list(hostnames), type='network_load')

        busy_hostnames = set()
        for rec in dl:
            if rec['job_id'] != self.parent_job.id:
                busy_hostnames.add(rec['host'])

        release_hostnames = hostnames - busy_hostnames
        if release_hostnames:
            try:
                for hostname in release_hostnames:
                    inventory.remove_net_monitoring_downtime(hostname)
            except Exception as e:
                logger.error(
                    'Job {job_id}, task {task_id}: failed to remove net monitoring downtime: '
                    '{error}'.format(
                        job_id=self.parent_job.id,
                        task_id=self.id,
                        error=e
                    )
                )
                raise

        try:
            res = processor.downtimes.remove({'job_id': self.parent_job.id,
                                              'host': {'$in': list(hostnames)},
                                              'type': 'network_load'})
            if res['ok'] != 1:
                raise ValueError('bad response: {0}'.format(res))
        except Exception as e:
            logger.error(
                'Job {job_id}, task {task_id}: unexpected mongo error: {error}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    error=e
                )
            )
            raise
