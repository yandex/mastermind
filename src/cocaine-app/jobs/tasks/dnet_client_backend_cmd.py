import logging

import infrastructure
from jobs import TaskTypes, RetryError
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class DnetClientBackendCmdTask(MinionCmdTask):
    """
    Minion task for executing dnet_client backend command

    Commands 'params' can contain the following keys:
        host: ip address of the elliptics node
        port: port of the elliptics node
        family: family of the elliptics node
        command: dnet_client subcommand (enable, disable, etc.)
        backend_id: dnet_client command target backend id

    All these params are required to run the command, but if
    not all of them are available to mastermind, it can supply
    additional parameters so that minion is able to get
    required ones from them.

    For example, when a newly created group should be run,
    mastermind has no way of knowing the backend id, but it
    can provide minion with config path and group id. Minion
    will open config file and find corresponding backend id by
    group id, and will then successfully run the command.
    """

    PARAMS = MinionCmdTask.PARAMS

    def __init__(self, job):
        super(DnetClientBackendCmdTask, self).__init__(job)
        self.cmd = TaskTypes.TYPE_DNET_CLIENT_BACKEND_CMD
        self.type = TaskTypes.TYPE_DNET_CLIENT_BACKEND_CMD

    def execute(self, processor):
        self.params['cmd_tpl'] = infrastructure.DNET_CLIENT_BACKEND_CMD_TPL
        self.params['subcommand'] = 'backend'
        try:
            minion_response = processor.minions.dnet_client_cmd(
                self.host,
                self.params
            )
        except RuntimeError as e:
            raise RetryError(self.attempts, e)
        self._set_minion_task_parameters(minion_response)
