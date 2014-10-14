from jobs.job_types import TaskTypes

from task import Task
from node_stop import NodeStopTask
from minion_cmd import MinionCmdTask
from history_remove_node import HistoryRemoveNodeTask
from recover_group_dc import RecoverGroupDcTask
from node_backend_defrag import NodeBackendDefragTask
from couple_defrag_state_check import CoupleDefragStateCheckTask


class TaskFactory(object):

    @staticmethod
    def make_task(data, job):
        task_type = data.get('type', None)
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
        raise ValueError('Unknown task type {0}'.format(task_type))
