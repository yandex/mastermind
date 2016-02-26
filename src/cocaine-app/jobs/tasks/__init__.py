from jobs.job_types import TaskTypes

from task import Task
from node_stop import NodeStopTask
from minion_cmd import MinionCmdTask
from history_remove_node import HistoryRemoveNodeTask
from recover_group_dc import RecoverGroupDcTask
from node_backend_defrag import NodeBackendDefragTask
from couple_defrag_state_check import CoupleDefragStateCheckTask
from rsync_backend import RsyncBackendTask
from create_group import CreateGroupTask
from remove_group import RemoveGroupTask
from dnet_client_backend_cmd import DnetClientBackendCmdTask


class TaskFactory(object):

    @staticmethod
    def make_task(data, job):
        task_type = data.get('type')
        if task_type == TaskTypes.TYPE_NODE_STOP_TASK:
            return NodeStopTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_MINION_CMD:
            return MinionCmdTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_HISTORY_REMOVE_NODE:
            return HistoryRemoveNodeTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_RECOVER_DC_GROUP_TASK:
            return RecoverGroupDcTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_NODE_BACKEND_DEFRAG_TASK:
            return NodeBackendDefragTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_COUPLE_DEFRAG_STATE_CHECK_TASK:
            return CoupleDefragStateCheckTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_RSYNC_BACKEND_TASK:
            return RsyncBackendTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_CREATE_GROUP:
            return CreateGroupTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_REMOVE_GROUP:
            return RemoveGroupTask.from_data(data, job)
        if task_type == TaskTypes.TYPE_DNET_CLIENT_BACKEND_CMD:
            return DnetClientBackendCmdTask.from_data(data, job)
        raise ValueError('Unknown task type {0}'.format(task_type))
