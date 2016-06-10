import random

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
import tasks
import storage


class AddLrcGroupsetJob(Job):
    PARAMS = (
        'metakey',
        'groups',
        'couple',
        'part_size',
        'scheme',
        'resources',
    )

    def __init__(self, **kwargs):
        super(AddLrcGroupsetJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_ADD_LRC_GROUPSET_JOB

    def _set_resources(self):
        resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
        }

        couple = storage.couples[self.couple]
        for group in couple.groups:
            nb = group.node_backends[0]
            # for replicas groupset we require IN and OUT network traffic since recovery
            # will be run for these groups
            resources[Job.RESOURCE_HOST_OUT].append(nb.node.host.addr)
            resources[Job.RESOURCE_HOST_IN].append(nb.node.host.addr)
            resources[Job.RESOURCE_FS].append(
                (nb.node.host.addr, str(nb.fs.fsid))
            )

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
        if self.couple not in storage.couples:
            raise JobBrokenError('Couple {} is not found'.format(self.couple))

        couple = storage.couples[self.couple]
        if couple.status not in storage.GOOD_STATUSES:
            raise JobBrokenError(
                'Couple {couple} has unexpected status {status}, allowed statuses are '
                '{allowed_statuses}'.format(
                    couple=couple,
                    status=couple.status,
                    allowed_statuses=storage.GOOD_STATUSES,
                )
            )

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

        if self.scheme == storage.Lrc.Scheme822v1.ID and couple.lrc822v1_groupset:
            raise JobBrokenError(
                'Couple {couple} already has {groupset_id} groupset'.format(
                    couple=couple,
                    groupset_id=storage.Lrc.Scheme822v1.ID,
                )
            )

    def create_tasks(self):
        """Create tasks for adding new lrc groupset to a couple

        - freeze couple;
        - run recover dc on replicas groupset;
        - make replicas groupset read-only;
        - run 'lrc convert' and 'lrc validate' to check convertion results;
        - write metakey to the new groupset;
        - wait till mastermind discovers the new groupset;
        - make replicas groupset writable;
        - unfreeze couple;
        """

        couple = storage.couples[self.couple]
        tasks = []

        tasks.append(
            self._freeze_couple_task(
                couple=couple,
            )
        )

        trace_id = self.id[:16]
        tasks.append(
            self._recover_dc_task(
                couple=couple,
                trace_id=trace_id,
            )
        )

        tasks.extend(
            self._make_groups_readonly_tasks(
                groups=couple.groups,
            )
        )

        new_groups = [storage.groups[group_id] for group_id in self.groups]
        tasks.extend(
            self._lrc_convert_tasks(
                src_groups=couple.groups,
                dst_groups=new_groups,
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

        tasks.extend(
            self._make_groups_writable_tasks(
                groups=couple.groups,
            )
        )

        tasks.append(
            self._unfreeze_couple_task(
                couple=self.couple,
            )
        )

        self.tasks = tasks

    def _freeze_couple_task(self, couple):
        return tasks.ChangeCoupleFrozenStatusTask.new(
            self,
            couple=str(couple),
            frozen=True,
        )

    def _recover_dc_task(self, couple, trace_id):
        # recovery is run on the group with the least number of keys to minimize network
        # utilization
        min_keys_group = min(couple.groups, key=lambda g: g.get_stat().files)
        nb = min_keys_group.node_backends[0]

        recover_cmd = infrastructure._recover_group_cmd(
            min_keys_group.group_id,
            trace_id=trace_id,
        )
        task = tasks.RecoverGroupDcTask.new(
            self,
            group=min_keys_group.group_id,
            host=nb.node.host.addr,
            cmd=recover_cmd,
            params={
                'node_backend': self.node_backend(
                    host=nb.node.host.addr,
                    port=nb.node.port,
                    family=nb.node.family,
                    backend_id=nb.backend_id,
                ),
                'group': str(min_keys_group.group_id)
            },
        )
        return task

    def _make_groups_readonly_tasks(self, groups):

        job_tasks = []

        for group in groups:
            for node_backend in group.node_backends:
                make_readonly_cmd = infrastructure._make_readonly_node_backend_cmd(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                )

                # create marker file on a filesystem preventing backend from enabling
                # when node is restarted
                mark_backend_path = self.make_path(
                    self.BACKEND_DOWN_MARKER,
                    base_path=node_backend.base_path
                ).format(
                    backend_id=node_backend.backend_id,
                )

                task = tasks.MinionCmdTask.new(
                    self,
                    host=node_backend.node.host.addr,
                    cmd=make_readonly_cmd,
                    params={
                        'node_backend': self.node_backend(
                            host=node_backend.node.host.addr,
                            port=node_backend.node.port,
                            family=node_backend.node.family,
                            backend_id=node_backend.backend_id,
                        ),
                        'mark_backend': mark_backend_path,
                        'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS],
                    }
                )
                job_tasks.append(task)

        return job_tasks

    def _lrc_convert_tasks(self, src_groups, dst_groups, part_size, scheme, trace_id):

        job_tasks = []

        # randomly select group where convert process will be executed
        exec_group = random.choice(dst_groups)
        node_backend = exec_group.node_backends[0]

        convert_cmd = infrastructure._lrc_convert_cmd(
            couple=self.couple,
            src_groups=src_groups,
            dst_groups=dst_groups,
            part_size=part_size,
            scheme=scheme,
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

        validate_cmd = infrastructure._lrc_validate_cmd(
            couple=self.couple,
            src_groups=src_groups,
            dst_groups=dst_groups,
            part_size=part_size,
            scheme=scheme,
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

    def _make_groups_writable_tasks(self, groups):

        job_tasks = []

        for group in groups:
            for node_backend in group.node_backends:
                make_writable_cmd = infrastructure._make_writable_node_backend_cmd(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                )

                # remove marker file on a filesystem preventing backend from enabling
                # when node is restarted
                mark_backend_path = self.make_path(
                    self.BACKEND_DOWN_MARKER,
                    base_path=node_backend.base_path
                ).format(
                    backend_id=node_backend.backend_id,
                )

                node_backend_id = self.node_backend(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                )

                task = tasks.MinionCmdTask.new(
                    self,
                    host=node_backend.node.host.addr,
                    cmd=make_writable_cmd,
                    params={
                        'node_backend': node_backend_id,
                        'unmark_backend': mark_backend_path,
                        'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS],
                    },
                )
                job_tasks.append(task)

                reconfigure_cmd = infrastructure._reconfigure_node_cmd(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                )

                task = tasks.MinionCmdTask.new(
                    self,
                    host=node_backend.node.host.addr,
                    cmd=reconfigure_cmd,
                    params={
                        'node_backend': node_backend_id,
                    },
                )

                job_tasks.append(task)

                # This start command is executed for the case when elliptics
                # was restarted between make_readonly and make_writable commands
                start_cmd = infrastructure._enable_node_backend_cmd(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                )

                task = tasks.MinionCmdTask.new(
                    self,
                    host=node_backend.node.host.addr,
                    cmd=start_cmd,
                    params={
                        'node_backend': node_backend_id,
                        'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]
                    },
                )

                job_tasks.append(task)

        return job_tasks

    def _unfreeze_couple_task(self, couple):
        return tasks.ChangeCoupleFrozenStatusTask.new(
            self,
            couple=couple,
            frozen=False,
        )

    @property
    def _involved_groups(self):
        couple = storage.couples[self.couple]
        return list(self.groups) + [g.group_id for g in couple.groups]

    @property
    def _involved_couples(self):
        return [self.couple]
