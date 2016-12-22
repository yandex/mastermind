import inventory
from jobs import TaskTypes
from minion_cmd import MinionCmdTask


class ExternalStorageTask(MinionCmdTask):

    def __init__(self, job):
        super(ExternalStorageTask, self).__init__(job)
        self.type = TaskTypes.TYPE_EXTERNAL_STORAGE

    @property
    def next_retry_ts(self):
        last_record = self.last_run_history_record
        if last_record.status != 'error':
            return None

        assert hasattr(self.parent_job, 'src_storage')
        assert hasattr(self.parent_job, 'src_storage_options')

        return inventory.external_storage_task_retry_ts(
            self,
            self.parent_job.src_storage,
            self.parent_job.src_storage_options
        )
