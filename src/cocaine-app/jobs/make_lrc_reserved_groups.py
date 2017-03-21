import os.path

from error import JobBrokenError
import infrastructure
import inventory
from job import Job
from job_types import JobTypes
import storage
import tasks


class MakeLrcReservedGroupsJob(Job):

    PARAMS = (
        'uncoupled_group',
        'dc',  # to skip determining dc when accounting running jobs during planning
        'host',  # to skip determining host when accounting running jobs during planning
        'lrc_groups',
        'metakey',
        'scheme',
        'resources',
        'total_space',
    )

    def __init__(self, **kwargs):
        super(MakeLrcReservedGroupsJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB

    def _set_resources(self):
        self.resources = {}

    @property
    def _required_group_types(self):
        return {
            self.uncoupled_group: storage.Group.TYPE_UNCOUPLED,
        }

    def create_tasks(self, processor):
        """ Create tasks for new lrc reserved groups construction
        This job prepares lrc groups that can later be used for lrc groupset restoring.
        This job involves several tasks:
            1) disassemble uncoupled group;
            2) create a number of lrc reserved groups (usually one uncoupled group is
                splitted into 8 lrc reserved groups in lrc-8-2-2 scheme);
            3) enable newly created lrc reserved groups;
            4) write metakey to newly created lrc reserved groups;
        """
        if self.uncoupled_group not in storage.groups:
            raise JobBrokenError('Uncoupled group {} is not found'.format(self.uncoupled_group))
        uncoupled_group = storage.groups[self.uncoupled_group]
        if uncoupled_group.type != storage.Group.TYPE_UNCOUPLED:
            raise JobBrokenError(
                "Group's {group} type is {current_type}, "
                "expected type is {expected_type}".format(
                    group=uncoupled_group,
                    current_type=uncoupled_group.type,
                    expected_type=storage.Group.TYPE_UNCOUPLED,
                )
            )

        self.tasks.extend(
            self._remove_uncoupled_group_tasks(uncoupled_group)
        )

        nb = uncoupled_group.node_backends[0]
        self.tasks.extend(
            self._create_new_lrc_groups_tasks(
                node_backend=nb,
                lrc_group_ids=self.lrc_groups,
            )
        )

        self.tasks.extend(
            self._enable_new_lrc_groups_tasks(
                node_backend=nb,
                lrc_group_ids=self.lrc_groups,
            )
        )

        self.tasks.extend(
            self._write_metakey_to_new_reserved_lrc_groups_tasks(
                lrc_group_ids=self.lrc_groups,
            )
        )

    def _remove_uncoupled_group_tasks(self, uncoupled_group):
        job_tasks = []
        nb = uncoupled_group.node_backends[0]

        remove_cmd = infrastructure.infrastructure._remove_node_backend_cmd(
            host=nb.node.host.addr,
            port=nb.node.port,
            family=nb.node.family,
            backend_id=nb.backend_id,
        )
        job_tasks.append(
            tasks.MinionCmdTask.new(
                self,
                host=nb.node.host.addr,
                cmd=remove_cmd,
                params={
                    'node_backend': str(nb).encode('utf-8'),
                },
            )
        )

        # remove uncoupled group from a file system
        job_tasks.append(
            tasks.RemoveGroupTask.new(
                self,
                group=uncoupled_group.group_id,
                host=nb.node.host.addr,
                params={
                    'group': str(uncoupled_group.group_id),
                    'group_base_path': nb.base_path,
                },
            )
        )

        job_tasks.append(
            tasks.HistoryRemoveNodeTask.new(
                self,
                group=uncoupled_group.group_id,
                host=nb.node.host.addr,
                port=nb.node.port,
                family=nb.node.family,
                backend_id=nb.backend_id,
            )
        )

        return job_tasks

    def _create_new_lrc_groups_tasks(self, node_backend, lrc_group_ids):
        job_tasks = []

        group_base_path_root_dir = os.path.dirname(node_backend.base_path.rstrip('/'))

        for lrc_group_id in lrc_group_ids:
            # create new group on a file system
            task = tasks.CreateGroupTask.new(
                self,
                group=lrc_group_id,
                host=node_backend.node.host.addr,
                params={
                    'total_space': self.total_space,
                    'group': str(lrc_group_id),
                    'group_base_path_root_dir': group_base_path_root_dir,
                },
            )
            job_tasks.append(task)

        # reconfigure elliptics node
        reconfigure_cmd = infrastructure.infrastructure._reconfigure_node_cmd(
            node_backend.node.host.addr,
            node_backend.node.port,
            node_backend.node.family
        )

        task = tasks.MinionCmdTask.new(
            self,
            host=node_backend.node.host.addr,
            cmd=reconfigure_cmd,
            params={'node_backend': str(node_backend).encode('utf-8')},
        )
        job_tasks.append(task)

        return job_tasks

    def _enable_new_lrc_groups_tasks(self, node_backend, lrc_group_ids):
        job_tasks = []

        for lrc_group_id in lrc_group_ids:
            # enable backend of the newly created group
            task = tasks.DnetClientBackendCmdTask.new(
                self,
                group=lrc_group_id,
                host=node_backend.node.host.addr,
                params={
                    'host': node_backend.node.host.addr,
                    'port': node_backend.node.port,
                    'family': node_backend.node.family,
                    'dnet_client_command': 'enable',
                    'group': lrc_group_id,
                    'config_path': inventory.get_node_config_path(node_backend.node),
                    'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS],
                },
            )
            job_tasks.append(task)

        return job_tasks

    def _write_metakey_to_new_reserved_lrc_groups_tasks(self, lrc_group_ids):
        job_tasks = []

        for lrc_group_id in lrc_group_ids:
            # write metakey to a new group
            task = tasks.WriteMetaKeyTask.new(
                self,
                group=lrc_group_id,
                metakey=storage.Group.compose_reserved_lrc_group_meta(
                    scheme=storage.Lrc.Scheme822v1,
                ),
            )
            job_tasks.append(task)

        return job_tasks

    @property
    def _involved_groups(self):
        return list(self.lrc_groups) + [self.uncoupled_group]

    @property
    def _involved_couples(self):
        return []

    @property
    def involved_uncoupled_groups(self):
        return [self.uncoupled_group]
