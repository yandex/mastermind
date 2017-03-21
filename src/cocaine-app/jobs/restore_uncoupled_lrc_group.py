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


class RestoreUncoupledLrcGroupJob(Job):
    PARAMS = (
        'group',
        'lrc_reserve_group',
        'lrc_groups',
        'group_is_cleaned',
        'scheme',
        'resources',
    )

    GROUP_FILE_PATH = config.get('restore', {}).get('group_file')

    def __init__(self, **kwargs):
        super(RestoreUncoupledLrcGroupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RESTORE_UNCOUPLED_LRC_GROUP_JOB

    def _set_resources(self):
        self.resources = {}

    @property
    def _required_group_types(self):
        return {
            self.lrc_reserve_group: storage.Group.TYPE_RESERVED_LRC_8_2_2_V1,
        }

    def create_tasks(self, processor):

        if not self.group_is_cleaned:
            self.tasks.extend(
                self._remove_old_group_tasks()
            )
        self.tasks.extend(
            self._remove_lrc_reserve_group_tasks()
        )
        self.tasks.extend(
            self._create_new_lrc_group_tasks()
        )

        self.tasks.extend(
            self._write_metakey_to_restored_group_task(processor)
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
            tasks.MinionCmdTask.new(
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

    def _remove_lrc_reserve_group_tasks(self):

        job_tasks = []

        group = storage.groups[self.lrc_reserve_group]

        if len(group.node_backends) != 1:
            raise JobBrokenError(
                'Uncoupled group {} has {} backends, expected 1 backend'.format(
                    group,
                    len(group.node_backends),
                )
            )

        nb = group.node_backends[0]

        job_tasks.append(
            tasks.RemoveGroupTask.new(
                self,
                group=group.group_id,
                host=nb.node.host.addr,
                params={
                    'group': str(group.group_id),
                    'group_base_path': nb.base_path,
                },
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
                group=group.group_id,
                host=nb.node.host.addr,
                cmd=remove_backend_cmd,
                params={
                    'group': str(group.group_id),
                    'success_codes': [self.DNET_CLIENT_UNKNOWN_BACKEND]
                },
            )
        )

        job_tasks.append(
            tasks.HistoryRemoveNodeTask.new(
                self,
                group=group.group_id,
                host=nb.node.host.addr,
                port=nb.node.port,
                family=nb.node.family,
                backend_id=nb.backend_id,
            )
        )

        return job_tasks

    def _create_new_lrc_group_tasks(self):
        job_tasks = []

        reserve_group = storage.groups[self.lrc_reserve_group]
        nb = reserve_group.node_backends[0]
        group_base_path_root_dir = os.path.dirname(nb.base_path.rstrip('/'))

        total_space = nb.stat.total_space

        job_tasks.append(
            tasks.CreateGroupTask.new(
                self,
                group=self.group,
                host=nb.node.host.addr,
                params={
                    'total_space': total_space,
                    'group': str(self.group),
                    'group_base_path_root_dir': group_base_path_root_dir,
                },
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

        job_tasks.append(
            tasks.DnetClientBackendCmdTask.new(
                self,
                group=self.group,
                host=nb.node.host.addr,
                params={
                    'host': nb.node.host.addr,
                    'port': nb.node.port,
                    'family': nb.node.family,
                    'dnet_client_command': 'enable',
                    'group': self.group,
                    'config_path': inventory.get_node_config_path(nb.node),
                    'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS],
                },
            )
        )

        return job_tasks

    def _write_metakey_to_restored_group_task(self, processor):
        job_tasks = []

        metakey = storage.Group.compose_uncoupled_lrc_group_meta(
            lrc_groups=self.lrc_groups,
            scheme=storage.Lrc.make_scheme(self.scheme),
        )

        job_tasks.append(
            tasks.WriteMetaKeyTask.new(
                self,
                group=self.group,
                metakey=metakey,
            )
        )

        return job_tasks

    @property
    def _involved_groups(self):
        return self.lrc_groups + [self.lrc_reserve_group]

    @property
    def _involved_couples(self):
        return []
