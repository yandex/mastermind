import logging
import os.path

from error import JobBrokenError
from infrastructure import infrastructure
import inventory
from job import Job
from job_types import JobTypes
from mastermind_core.config import config
import storage
import tasks


logger = logging.getLogger('mm.jobs')

LRC_CFG = config.get('lrc', {}).get('lrc-8-2-2-v1', {})
LRC_RESTORE_CFG = LRC_CFG.get('restore', {})


class RestoreLrcGroupJob(Job):
    PARAMS = (
        'group',
        'lrc_reserve_group',
        'couple',
        'lrc_groupset',
        'resources',
    )

    GROUP_FILE_PATH = config.get('restore', {}).get('group_file')

    def __init__(self, **kwargs):
        super(RestoreLrcGroupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RESTORE_LRC_GROUP_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(RestoreLrcGroupJob, cls).new(*args, **kwargs)
        try:
            lrc_groupset = storage.groups[job.group].couple
            job.lrc_groupset = str(lrc_groupset)
            job.couple = str(lrc_groupset.couple)
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

        nb = storage.groups[self.lrc_reserve_group].node_backends[0]

        resources[Job.RESOURCE_HOST_IN].append(nb.node.host.addr)
        resources[Job.RESOURCE_FS].append(
            (nb.node.host.addr, str(nb.fs.fsid))
        )

        self.resources = resources

    @property
    def _required_group_types(self):
        return {
            self.lrc_reserve_group: storage.Group.TYPE_RESERVED_LRC_8_2_2_V1,
        }

    def create_tasks(self, processor):

        self.tasks.extend(
            self._lrc_recover_tasks()
        )

        self.tasks.extend(
            self._remove_old_group_tasks()
        )

        self.tasks.extend(
            self._replace_lrc_reserve_group_tasks()
        )

        self.tasks.extend(
            self._write_metakey_to_restored_group_task(processor)
        )

        if LRC_RESTORE_CFG.get('external_storage_validation', False):
            self.tasks.extend(
                self._lrc_validate_task(processor)
            )

    def _remove_old_group_tasks(self):
        job_tasks = []

        nb = infrastructure.get_backend_by_group_id(self.group)

        if nb is None:
            logger.warn(
                'Job {job_id}: lrc group {group} does not have any known backends, '
                'tasks for removing the old backend will be skipped'.format(
                    job_id=self.id,
                    group=self.group,
                )
            )
            return job_tasks

        group_file = (
            os.path.join(nb.base_path, self.GROUP_FILE_PATH)
            if self.GROUP_FILE_PATH else
            ''
        )

        move_path = (
            os.path.join(nb.base_path, self.GROUP_FILE_DIR_MOVE_SRC_RENAME)
            if self.GROUP_FILE_DIR_MOVE_SRC_RENAME and group_file else
            ''
        )

        if move_path:
            stop_restore_backend = self.make_path(
                self.BACKEND_STOP_MARKER,
                base_path=nb.base_path
            ).format(
                backend_id=nb.backend_id,
            )

            job_tasks.append(
                tasks.MovePathTask.new(
                    self,
                    host=nb.node.host.addr,
                    params={
                        'move_src': os.path.join(os.path.dirname(group_file)),
                        'move_dst': move_path,
                        'stop_backend': stop_restore_backend,
                    }
                )
            )

        remove_backend_cmd = infrastructure._remove_node_backend_cmd(
            nb.node.host.addr,
            nb.node.port,
            nb.node.family,
            nb.backend_id,
        )
        job_tasks.append(
            tasks.NodeStopTask.new(
                self,
                group=self.group,
                host=nb.node.host.addr,
                cmd=remove_backend_cmd,
                params={
                    'group': str(self.group),
                    'success_codes': [self.DNET_CLIENT_UNKNOWN_BACKEND]
                },
            )
        )

        job_tasks.append(
            tasks.HistoryRemoveNodeTask.new(
                self,
                group=self.group,
                host=nb.node.host.addr,
                port=nb.node.port,
                family=nb.node.family,
                backend_id=nb.backend_id,
            )
        )

        return job_tasks

    def _replace_lrc_reserve_group_tasks(self):

        job_tasks = []

        lrc_reserve_group = storage.groups[self.lrc_reserve_group]

        if len(lrc_reserve_group.node_backends) != 1:
            raise JobBrokenError(
                'Lrc reserve group {} has {} backends, expected 1 backend'.format(
                    lrc_reserve_group,
                    len(lrc_reserve_group.node_backends),
                )
            )

        nb = lrc_reserve_group.node_backends[0]

        disable_backend_cmd = infrastructure._disable_node_backend_cmd(
            nb.node.host.addr,
            nb.node.port,
            nb.node.family,
            nb.backend_id,
        )
        job_tasks.append(
            tasks.MinionCmdTask.new(
                self,
                group=lrc_reserve_group.group_id,
                host=nb.node.host.addr,
                cmd=disable_backend_cmd,
                params={
                    'group': str(lrc_reserve_group.group_id),
                    'success_codes': [self.DNET_CLIENT_UNKNOWN_BACKEND]
                },
            )
        )

        job_tasks.append(
            tasks.HistoryRemoveNodeTask.new(
                self,
                group=lrc_reserve_group.group_id,
                host=nb.node.host.addr,
                port=nb.node.port,
                family=nb.node.family,
                backend_id=nb.backend_id,
            )
        )

        broken_group = storage.groups[self.group]

        group_file = (
            os.path.join(nb.base_path, self.GROUP_FILE_PATH)
            if self.GROUP_FILE_PATH else
            ''
        )

        job_tasks.append(
            tasks.CreateGroupFileTask.new(
                self,
                group=broken_group.group_id,
                host=nb.node.host.addr,
                params={
                    'group': str(broken_group.group_id),
                    'group_file': group_file,
                }
            )
        )

        # reconfigure elliptics node
        reconfigure_cmd = infrastructure._reconfigure_node_cmd(
            nb.node.host.addr,
            nb.node.port,
            nb.node.family
        )

        job_tasks.append(
            tasks.MinionCmdTask.new(
                self,
                host=nb.node.host.addr,
                cmd=reconfigure_cmd,
                params={'node_backend': str(nb).encode('utf-8')},
            )
        )

        enable_backend_cmd = infrastructure._enable_node_backend_cmd(
            nb.node.host.addr,
            nb.node.port,
            nb.node.family,
            nb.backend_id,
        )
        job_tasks.append(
            tasks.MinionCmdTask.new(
                self,
                group=broken_group.group_id,
                host=nb.node.host.addr,
                cmd=enable_backend_cmd,
                params={
                    'group': str(broken_group.group_id),
                    'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]
                },
            )
        )

        return job_tasks

    def _lrc_recover_tasks(self):

        job_tasks = []

        broken_group = storage.groups[self.group]
        lrc_groupset = broken_group.couple

        if not isinstance(lrc_groupset, storage.Lrc822v1Groupset):
            raise JobBrokenError(
                'Group {group} belongs to a non-lrc groupset {groupset} ({type})'.format(
                    group=broken_group,
                    groupset=lrc_groupset,
                    type=type(lrc_groupset).__name__,
                )
            )

        lrc_reserve_group = storage.groups[self.lrc_reserve_group]
        nb = lrc_reserve_group.node_backends[0]

        recover_lrc_index_shard_cmd = infrastructure._recover_lrc_index_shard_cmd(
            lrc_groupset=lrc_groupset,
            broken_group=broken_group,
            lrc_reserve_group=lrc_reserve_group,
            json_stats=True,
            trace_id=self.id[:16],
        )

        job_tasks.append(
            tasks.RecoverGroupDcTask.new(
                self,
                group=self.group,
                host=nb.node.host.addr,
                cmd=recover_lrc_index_shard_cmd,
                params={
                    'node_backend': self.node_backend(
                        host=nb.node.host.addr,
                        port=nb.node.port,
                        backend_id=nb.backend_id,
                    ),
                    'group': str(self.group),
                }
            )
        )

        recover_cmd = infrastructure._lrc_recovery_cmd(
            lrc_groupset=lrc_groupset,
            copy_groups=[(broken_group, lrc_reserve_group)],
            trace_id=self.id[:16],
            json_stats=True,
        )

        job_tasks.append(
            tasks.LrcRecoveryTask.new(
                self,
                group=self.group,
                host=nb.node.host.addr,
                cmd=recover_cmd,
                params={
                    'node_backend': self.node_backend(
                        host=nb.node.host.addr,
                        port=nb.node.port,
                        backend_id=nb.backend_id,
                    ),
                    'group': str(self.group),
                }
            )
        )

        return job_tasks

    def _write_metakey_to_restored_group_task(self, processor):
        job_tasks = []

        lrc_groupset = storage.groupsets[self.lrc_groupset]

        metakey = lrc_groupset.compose_group_meta(
            couple=lrc_groupset.couple,
            settings=lrc_groupset.groupset_settings,
        )

        job_tasks.append(
            tasks.WriteMetaKeyTask.new(
                self,
                group=self.group,
                metakey=metakey,
            )
        )

        return job_tasks

    def _lrc_validate_task(self, processor):
        job_tasks = []

        broken_group = storage.groups[self.group]
        lrc_groupset = broken_group.couple
        couple = lrc_groupset.couple

        mappings = processor.external_storage_meta.mapping_list(
            couple=[couple.couple_id]
        )

        if len(mappings) == 0:
            logger.debug(
                'Failed to find external storage mapping for couple {}, couple was not converted '
                'from external storage'.format(
                    couple
                )
            )
            return []

        mapping = mappings[0]

        dst_groups = []
        for couple_id in mapping.couples:
            mapped_couple = storage.couples[str(couple_id)]
            dst_groups.append(mapped_couple.lrc822v1_groupset.groups)

        lrc_reserve_group = storage.groups[self.lrc_reserve_group]
        nb = lrc_reserve_group.node_backends[0]

        validate_cmd = inventory.make_external_storage_validate_command(
            dst_groups=dst_groups,
            groupset_type=storage.GROUPSET_LRC,
            groupset_settings=lrc_groupset.groupset_settings,
            src_storage=mapping.external_storage,
            src_storage_options=mapping.external_storage_options,
            additional_backends=[nb],
            trace_id=self.id[:16],
        )

        job_tasks.append(
            tasks.ExternalStorageTask.new(
                self,
                host=nb.node.host.addr,
                cmd=validate_cmd,
            )
        )

        return job_tasks

    @property
    def _involved_groups(self):
        group = storage.groups[self.group]
        return [g for g in group.couple.groups] + [self.lrc_reserve_group]

    @property
    def _involved_couples(self):
        group = storage.groups[self.group]
        # NOTE: group.couple.couple is used to simulate group.groupset.couple
        return [str(group.couple.couple)]
