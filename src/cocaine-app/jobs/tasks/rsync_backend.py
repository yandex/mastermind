import logging

from infrastructure import infrastructure
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
        logger.info('Job {0}, task {1}: checking group {2} and node backend {3} '
            'state'.format(self.parent_job.id, self.id, self.group, self.node_backend))

        if not self.group in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(self.group))

        group = storage.groups[self.group]

        if self.node_backend:
            # Check if old backend is down
            # This check is not applied to move job
            if self.node_backend in storage.node_backends:
                logger.info('Job {0}, task {1}: checking node backend status'.format(
                    self.parent_job.id, self.id, self.node_backend))
                node_backend = storage.node_backends[self.node_backend]
                if (node_backend in group.node_backends and
                    node_backend.status not in (storage.Status.STALLED, storage.Status.INIT, storage.Status.RO)):

                    raise JobBrokenError('Node backend {0} has status, expected {1}'.format(
                        node_backend.status, (storage.Status.STALLED, storage.Status.INIT, storage.Status.RO)))

            elif len(group.node_backends) > 0:
                raise JobBrokenError('Group {0} is running on backend {1} which '
                    'does not match {2}'.format(self.group, str(group.node_backends[0]),
                        self.node_backend))

        super(RsyncBackendTask, self).execute(processor)

    def on_exec_start(self, processor):
        hostnames = set([infrastructure.get_hostname_by_addr(host)
                         for host in (self.host, self.src_host)])

        dl = jobs.Job.list(processor.downtimes,
                           host=hostnames, type='network_load')

        set_hostnames = set(record['host'] for record in dl)
        not_set_hostnames = hostnames - set_hostnames

        if not_set_hostnames:
            try:
                for hostname in not_set_hostnames:
                    inventory.set_net_monitoring_downtime(hostname)
            except Exception as e:
                logger.error('Job {0}, task {1}: failed to set net monitoring downtime: '
                    '{2}'.format(self.parent_job.id, self.id, e))
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
            logger.error('Job {0}, task {1}: unexpected mongo error: '
                '{2}'.format(self.parent_job.id, self.id, e))
            raise

    def on_exec_stop(self, processor):
        hostnames = set([infrastructure.get_hostname_by_addr(host)
                         for host in (self.host, self.src_host)])

        dl = jobs.Job.list(processor.downtimes,
                           host=hostnames, type='network_load')

        busy_hostnames = set()
        for rec in dl:
            if dl['job_id'] != self.parent_job.id:
                busy_hostnames.add(rec['host'])

        release_hostnames = hostnames - busy_hostnames
        if release_hostnames:
            try:
                for hostname in release_hostnames:
                    inventory.remove_net_monitoring_downtime(hostname)
            except Exception as e:
                logger.error('Job {0}, task {1}: failed to remove net monitoring downtime: '
                    '{2}'.format(self.parent_job.id, self.id, e))
                raise

        try:
            res = processor.downtimes.remove({'job_id': self.parent_job.id,
                                              'host': {'$in': list(hostnames)},
                                              'type': 'network_load'})
            if res['ok'] != 1:
                raise ValueError('bad response: {0}'.format(res))
        except Exception as e:
            logger.error('Job {0}, task {1}: unexpected mongo error: '
                '{2}'.format(self.parent_job.id, self.id, e))
            raise
