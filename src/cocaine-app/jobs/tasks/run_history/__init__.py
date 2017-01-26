
class RunHistoryRecord(object):

    __slots__ = ['data']

    START_TS = 'start_ts'
    FINISH_TS = 'finish_ts'
    STATUS = 'status'
    ARTIFACTS = 'artifacts'
    ERROR_MSG = 'error_msg'
    DELAYED_TILL_TS = 'delayed_till_ts'

    # for minion command tasks
    COMMAND_UID = 'command_uid'
    EXIT_CODE = 'exit_code'

    def __init__(self, data):
        self.data = data

    def _data_key(key, doc=None):
        def getter(self):
            return self.data[key]

        def setter(self, val):
            self.data[key] = val

        return property(getter, setter, doc=doc)

    start_ts = _data_key(START_TS, "Timestamp of task's execution start")
    finish_ts = _data_key(FINISH_TS, "Timestamp of task's execution finish")
    status = _data_key(STATUS, "Task status, either 'success' or 'error'")
    artifacts = _data_key(ARTIFACTS, "Task artifacts, custom dictionary for any task type")
    error_msg = _data_key(ERROR_MSG, "Task error message in case of an error, otherwise None")
    delayed_till_ts = _data_key(
        DELAYED_TILL_TS,
        "Timestamp when the task can be retried in case of an error"
    )

    command_uid = _data_key(COMMAND_UID, "Contains minion command uid")
    exit_code = _data_key(EXIT_CODE, "Contains minion command exit code")

    def dump(self):
        return self.data
