import itertools
import logging
import random

from infrastructure import infrastructure
import inventory
from job import Job
from job_types import JobTypes
import tasks
import storage


logger = logging.getLogger('mm.jobs')


class ConvertToLrcGroupsetJob(Job):
    PARAMS = (
        'ns',
        'groups',
        'mandatory_dcs',
        'part_size',
        'scheme',
        'determine_data_size',
        'src_storage',
        'src_storage_options',
        'resources',
    )

    def __init__(self, **kwargs):
        super(ConvertToLrcGroupsetJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        # TODO: for backward compatibility, remove after converting
        job = super(ConvertToLrcGroupsetJob, cls).new(*args, **kwargs)
        job.groups = cls._get_groups(job.groups)

        return job

    @staticmethod
    def _get_groups(groups):
        if groups and not isinstance(groups[0], (list, tuple)):
            # only one groupset support, change to multi-groupsets support
            return [groups]
        return groups

    def _set_resources(self):
        resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
        }

        # TODO: remove _get_groups usage after jobs converting
        for groupset_group_ids in self._get_groups(self.groups):
            for group_id in groupset_group_ids:
                group = storage.groups[group_id]
                nb = group.node_backends[0]
                # for groups of a future lrc groupset we require only IN network traffic
                # since data will be written to these groups
                resources[Job.RESOURCE_HOST_IN].append(nb.node.host.addr)
                resources[Job.RESOURCE_FS].append(
                    (nb.node.host.addr, str(nb.fs.fsid))
                )

        self.resources = resources

    def create_tasks(self, processor):
        """Create tasks for adding new lrc groupset to a couple

        If @determine_data_size is set:
            - create determine data size task;
        otherwise:
            - run 'lrc convert', then 'lrc validate' to check convertion results;
            - write corresponding metakeys to the new groupsets;
            - wait till mastermind discovers the new groupsets;
            - add mapping to couple id;
        """

        tasks = []

        trace_id = self.id[:16]

        if self.determine_data_size:
            tasks.append(
                self._determine_data_size_task(
                    src_storage=self.src_storage,
                    src_storage_options=self.src_storage_options,
                    part_size=self.part_size,
                    scheme=self.scheme,
                    trace_id=trace_id,
                )
            )
            # other tasks will be created on data size task completion
            self.tasks = tasks
            return

        dst_groups = [
            [storage.groups[group_id] for group_id in groupset_group_ids]
            for groupset_group_ids in self.groups
        ]
        tasks.extend(
            self._lrc_convert_tasks(
                dst_groups=dst_groups,
                src_storage=self.src_storage,
                src_storage_options=self.src_storage_options,
                part_size=self.part_size,
                scheme=self.scheme,
                trace_id=trace_id,
            )
        )

        couple_ids = self._generate_couple_ids(count=len(dst_groups))

        tasks.extend(
            self._write_metakeys_to_new_groups_tasks(
                couple_ids=couple_ids,
                groups=dst_groups,
                processor=processor,
            )
        )

        for groupset_group_ids in self.groups:
            groupset_id = ':'.join(str(g) for g in groupset_group_ids)
            tasks.append(
                self._wait_groupset_state_task(groupset=groupset_id)
            )

        tasks.append(
            self._write_external_storage_mapping_task(
                couple_ids=couple_ids,
            )
        )

        tasks.extend(
            self._read_preference_tasks(
                couple_ids=couple_ids,
            )
        )

        self.couples = couple_ids

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
        exec_group = random.choice(    # random group within groupset
            random.choice(dst_groups)  # random groupset
        )
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

    def _determine_data_size_task(self,
                                  src_storage,
                                  src_storage_options,
                                  part_size,
                                  scheme,
                                  trace_id=None):

        # randomly select node backend to run task (since we do not know any groupset
        # at this point)

        # TODO: do not make a copy of all node backends
        node_backend = random.choice(storage.node_backends.values())

        data_size_cmd = inventory.make_external_storage_data_size_command(
            groupset_type=storage.GROUPSET_LRC,
            groupset_settings={
                'scheme': self.scheme,
                'part_size': self.part_size,
            },
            src_storage=self.src_storage,
            src_storage_options=self.src_storage_options,
            trace_id=trace_id,
        )

        return tasks.ExternalStorageDataSizeTask.new(
            self,
            host=node_backend.node.host.addr,
            cmd=data_size_cmd,
            params={
                'node_backend': self.node_backend(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                ),
            },
            groupset_type=storage.GROUPSET_LRC,
            mandatory_dcs=self.mandatory_dcs,
        )

    def _get_namespace(self):
        if self.ns not in storage.namespaces:
            ns = storage.namespaces.add(self.ns)
        else:
            ns = storage.namespaces[self.ns]
        return ns

    @staticmethod
    def _generate_couple_ids(count):
        return infrastructure.reserve_group_ids(count)

    def _generate_metakey(self, couple_id, groups, processor):
        try:
            # create dummy couple to construct metakey
            couple = storage.Couple([storage.Group(couple_id)])
            ns = self._get_namespace()
            ns.add_couple(couple)
            try:
                settings = {
                    'scheme': self.scheme,
                    'part_size': self.part_size,
                }
                Groupset = storage.groupsets.make_groupset_type(
                    type=storage.GROUPSET_LRC,
                    settings=settings,
                )
                metakey = processor._compose_groupset_metakey(
                    groupset_type=Groupset,
                    groups=groups,
                    couple=couple,
                    settings=settings,
                )
            finally:
                # this also removes couple from namespace @ns
                couple.destroy()
        except Exception:
            logger.exception('Failed to construct metakey')
            raise
        return metakey

    def _write_metakeys_to_new_groups_tasks(self, couple_ids, groups, processor):
        job_tasks = []
        for couple_id, groupset_groups in itertools.izip(couple_ids, groups):
            metakey = self._generate_metakey(couple_id, groupset_groups, processor)
            for group in groupset_groups:
                # write corresponding metakey to a new group
                task = tasks.WriteMetaKeyTask.new(
                    self,
                    group=group.group_id,
                    metakey=metakey,
                )
                job_tasks.append(task)

        return job_tasks

    def _write_external_storage_mapping_task(self, couple_ids):
        return tasks.WriteExternalStorageMappingTask.new(
            self,
            external_storage=self.src_storage,
            external_storage_options=self.src_storage_options,
            couples=couple_ids,
            namespace=self.ns,
        )

    def _wait_groupset_state_task(self, groupset):
        return tasks.WaitGroupsetStateTask.new(
            self,
            groupset=groupset,
            groupset_status=storage.Status.ARCHIVED,
        )

    def _read_preference_tasks(self, couple_ids):
        job_tasks = []
        read_preference_settings = {
            storage.Couple.READ_PREFERENCE: [storage.Lrc.Scheme822v1.ID],
        }
        for couple_id in couple_ids:
            job_tasks.append(
                tasks.ChangeCoupleSettingsTask.new(
                    self,
                    # redundant, but consistent with couple id construction
                    couple=':'.join([str(couple_id)]),
                    settings=read_preference_settings,
                    update=True,
                )
            )
        return job_tasks

    @property
    def _involved_groups(self):
        return [
            g
            for groupset_groups in self.groups
            for g in groupset_groups
        ]

    @property
    def _involved_couples(self):
        return []
