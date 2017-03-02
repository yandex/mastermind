from collections import OrderedDict
import os.path
import logging

from error import JobBrokenError
import infrastructure
import inventory
from job import Job
from job_types import JobTypes
import tasks
import storage


logger = logging.getLogger('mm.jobs')


class MakeLrcGroupsJob(Job):

    PARAMS = (
        'uncoupled_groups',
        'lrc_groups',
        'resources',
        'total_space',
    )

    def __init__(self, **kwargs):
        super(MakeLrcGroupsJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_MAKE_LRC_GROUPS_JOB

    def _set_resources(self):
        self.resources = {}

    @property
    def _required_group_types(self):
        return {
            group_id: storage.Group.TYPE_UNCOUPLED
            for group_id in self.uncoupled_groups
        }

    def create_tasks(self, processor):
        """ Create tasks for new lrc groups construction

        This job prepares lrc groups that can later be used for groupset construction.
        This job involves several tasks:
            1) disassemble each uncoupled group from 'uncoupled_groups';
            2) in place of each uncoupled group create a fixed number of
                uncoupled lrc groups (usually one uncoupled group is replaced with
                8 lrc uncoupled groups in lrc-8-2-2 scheme);
            3) enable newly created lrc uncoupled groups;
            4) write metakey to newly created lrc uncoupled groups;

        NOTE: number of 'lrc_groups' should be divisible by the number of 'uncoupled_groups'
            since each uncoupled group is replaced by the same number of lrc uncoupled groups;

        NOTE: the default mode is to take 12 uncoupled groups and replace
            each one of them with 8 lrc uncoupled groups (since each lrc uncoupled group
            has total space of 1/8th of uncoupled group's total space);
        """

        if len(self.lrc_groups) % len(self.uncoupled_groups) != 0:
            raise JobBrokenError(
                'Number of lrc groups ({lrc_groups_count}) does not match '
                'the number of uncoupled groups ({uncoupled_groups_count})'.format(
                    lrc_groups_count=len(self.lrc_groups),
                    uncoupled_groups_count=len(self.uncoupled_groups),
                )
            )

        for group_id in self.uncoupled_groups:
            if group_id not in storage.groups:
                raise JobBrokenError('Uncoupled group {} is not found'.format(group_id))
            group = storage.groups[group_id]
            if group.type != storage.Group.TYPE_UNCOUPLED:
                raise JobBrokenError(
                    "Group's {group} type is {current_type}, "
                    "expected type is {expected_type}".format(
                        group=group_id,
                        current_type=group.type,
                        expected_type=storage.Group.TYPE_UNCOUPLED,
                    )
                )
        lrc_groups_count_per_host = len(self.lrc_groups) / len(self.uncoupled_groups)

        # 'lrc_groups_ids_by_uncoupled_group_id' maps uncoupled group id
        # to the list of lrc groups ids that will take its place.
        # E.g., if 96 group ids starting with 1001 are reserved for new lrc
        # groups, and uncoupled groups [5, 6, 7, ...] are selected, then this
        # dict is constructed as following:
        # {
        #     5: [1001, 1013, 1025, 1037, 1049, 1061, 1073, 1085],
        #     6: [1002, 1014, 1026, 1038, 1050, 1062, 1074, 1086],
        #     ...
        # }
        lrc_groups_ids_by_uncoupled_group_id = OrderedDict()

        for i, group in enumerate(self.uncoupled_groups):
            lrc_group_ids = [
                self.lrc_groups[i + j * len(self.uncoupled_groups)]
                for j in xrange(lrc_groups_count_per_host)
            ]
            lrc_groups_ids_by_uncoupled_group_id[group] = lrc_group_ids

        # lrc_groups_sets is a list of lists, where each nested
        # list consists of lrc groups that will later become LRC groupset,
        # e.g. [
        #       [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012],
        #       [1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024],
        #       ...
        #      ]
        lrc_groups_sets = []

        set_len = len(self.uncoupled_groups)
        for i in xrange(lrc_groups_count_per_host):
            lrc_groups_set = self.lrc_groups[i * set_len:(i + 1) * set_len]
            lrc_groups_sets.append(lrc_groups_set)

        for uncoupled_group_id, lrc_group_ids in lrc_groups_ids_by_uncoupled_group_id.iteritems():

            uncoupled_group = storage.groups[uncoupled_group_id]
            self.tasks.extend(
                self._remove_uncoupled_group_tasks(uncoupled_group)
            )

            nb = uncoupled_group.node_backends[0]
            self.tasks.extend(
                self._create_new_lrc_groups_tasks(
                    node_backend=nb,
                    lrc_group_ids=lrc_group_ids,
                )
            )

            self.tasks.extend(
                self._enable_new_lrc_groups_tasks(
                    node_backend=nb,
                    lrc_group_ids=lrc_group_ids,
                )
            )

            self.tasks.extend(
                self._write_metakey_to_new_lrc_groups_tasks(
                    lrc_group_ids=lrc_group_ids,
                    lrc_groups_sets=lrc_groups_sets,
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

        # remove nb from uncoupled group's history
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

    def _write_metakey_to_new_lrc_groups_tasks(self, lrc_group_ids, lrc_groups_sets):
        job_tasks = []

        for lrc_groups_set_id, lrc_group_id in enumerate(lrc_group_ids):
            # write metakey to a new group
            task = tasks.WriteMetaKeyTask.new(
                self,
                group=lrc_group_id,
                metakey=storage.Group.compose_uncoupled_lrc_group_meta(
                    lrc_groups=lrc_groups_sets[lrc_groups_set_id],
                    scheme=storage.Lrc.Scheme822v1,
                ),
            )
            job_tasks.append(task)

        return job_tasks

    @property
    def _involved_groups(self):
        return self.uncoupled_groups + self.lrc_groups

    @property
    def _involved_couples(self):
        return []

    @property
    def involved_uncoupled_groups(self):
        return self.uncoupled_groups
