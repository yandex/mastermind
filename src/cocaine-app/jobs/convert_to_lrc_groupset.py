import random

from error import JobBrokenError
from infrastructure import infrastructure
import inventory
from job import Job
from job_types import JobTypes
import tasks
import storage


class ConvertToLrcGroupsetJob(Job):
    PARAMS = (
        'couple',
        'groups',
        'metakey',
        'part_size',
        'scheme',
        'src_storage',
        'src_storage_options',
        'resources',
    )

    def __init__(self, **kwargs):
        super(ConvertToLrcGroupsetJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB

    def _set_resources(self):
        resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
        }

        for group_id in self.groups:
            group = storage.groups[group_id]
            nb = group.node_backends[0]
            # for groups of a future lrc groupset we require only IN network traffic
            # since data will be written to these groups
            resources[Job.RESOURCE_HOST_IN].append(nb.node.host.addr)
            resources[Job.RESOURCE_FS].append(
                (nb.node.host.addr, str(nb.fs.fsid))
            )

        self.resources = resources

    def _check_job(self):
        for group_id in self.groups:
            if group_id not in storage.groups:
                raise JobBrokenError('Group {} is not found'.format(group_id))
            group = storage.groups[group_id]
            if group.status != storage.Status.COUPLED:
                raise JobBrokenError(
                    'Group {group} has unexpected status {status}, allowed status is '
                    '{expected_status}'.format(
                        group=group,
                        status=group.status,
                        expected_status=storage.Status.COUPLED,
                    )
                )

        if self.couple in storage.couples:
            couple = storage.couples[self.couple]
            if self.scheme == storage.Lrc.Scheme822v1.ID and couple.lrc822v1_groupset:
                raise JobBrokenError(
                    'Couple {couple} already has {groupset_id} groupset'.format(
                        couple=couple,
                        groupset_id=storage.Lrc.Scheme822v1.ID,
                    )
                )

    def create_tasks(self):
        """Create tasks for adding new lrc groupset to a couple

        - run 'lrc convert', then 'lrc validate' to check convertion results;
        - write metakey to the new groupset;
        - wait till mastermind discovers the new groupset;
        - add mapping to couple id;
        """

        tasks = []

        trace_id = self.id[:16]

        new_groups = [storage.groups[group_id] for group_id in self.groups]
        tasks.extend(
            self._lrc_convert_tasks(
                dst_groups=new_groups,
                src_storage=self.src_storage,
                src_storage_options=self.src_storage_options,
                part_size=self.part_size,
                scheme=self.scheme,
                trace_id=trace_id,
            )
        )

        tasks.extend(
            self._write_metakey_to_new_groups_tasks(
                groups=new_groups,
                metakey=self.metakey,
            )
        )

        groupset_id = ':'.join(str(g) for g in self.groups)
        tasks.append(
            self._wait_groupset_state_task(groupset=groupset_id)
        )

        # TODO: add mapping task

        self.tasks = tasks

    def _lrc_convert_tasks(self,
                           dst_groups,
                           src_storage,
                           src_storage_options,
                           part_size,
                           scheme,
                           trace_id=None):

        job_tasks = []

        # randomly select group where convert process will be executed
        exec_group = random.choice(dst_groups)
        node_backend = exec_group.node_backends[0]

        convert_cmd = inventory.make_external_storage_convert_command(
            dst_groups=dst_groups,
            groupset_type=storage.GROUPSET_LRC,
            groupset_settings={
                'scheme': self.scheme,
                'part_size': self.part_size,
            },
            src_storage=self.src_storage,
            src_storage_options=self.src_storage_options,
            trace_id=trace_id,
        )

        task = tasks.MinionCmdTask.new(
            self,
            host=node_backend.node.host.addr,
            cmd=convert_cmd,
            params={
                'node_backend': self.node_backend(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                ),
            }
        )

        job_tasks.append(task)

        validate_cmd = inventory.make_external_storage_validate_command(
            dst_groups=dst_groups,
            groupset_type=storage.GROUPSET_LRC,
            groupset_settings={
                'scheme': self.scheme,
                'part_size': self.part_size,
            },
            src_storage=self.src_storage,
            src_storage_options=self.src_storage_options,
            trace_id=trace_id,
        )

        task = tasks.MinionCmdTask.new(
            self,
            host=node_backend.node.host.addr,
            cmd=validate_cmd,
            params={
                'node_backend': self.node_backend(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                ),
            }
        )

        job_tasks.append(task)

        return job_tasks

    def _write_metakey_to_new_groups_tasks(self, groups, metakey):
        job_tasks = []

        for group in groups:
            # write metakey to a new group
            task = tasks.WriteMetaKeyTask.new(
                self,
                group=group.group_id,
                metakey=metakey,
            )
            job_tasks.append(task)

        return job_tasks

    def _wait_groupset_state_task(self, groupset):
        return tasks.WaitGroupsetStateTask.new(
            self,
            groupset=groupset,
            groupset_status=storage.Status.ARCHIVED,
        )

    @property
    def _involved_groups(self):
        return self.groups

    @property
    def _involved_couples(self):
        return [self.couple]
