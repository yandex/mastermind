import inventory
from jobs import TaskTypes
from minion_cmd import MinionCmdTask
import storage


class ExternalStorageTask(MinionCmdTask):

    def __init__(self, job):
        super(ExternalStorageTask, self).__init__(job)
        self.type = TaskTypes.TYPE_EXTERNAL_STORAGE

    @property
    def next_retry_ts(self):
        last_record = self.last_run_history_record
        if last_record.status != 'error':
            return None

        # TODO: move src_storage and src_storage_options to task params
        if hasattr(self.parent_job, 'src_storage') and hasattr(self.parent_job, 'src_storage_options'):
            retry_ts = inventory.external_storage_task_retry_ts(
                self,
                self.parent_job.src_storage,
                self.parent_job.src_storage_options
            )

            if retry_ts:
                return retry_ts

        return super(ExternalStorageTask, self).next_retry_ts

    def ready_for_retry(self, processor):
        if super(ExternalStorageTask, self).ready_for_retry(processor):

            ready = inventory.is_external_storage_task_ready_for_retry(
                self,
                self.parent_job.src_storage,
                self.parent_job.src_storage_options,
                storage,
                processor
            )

            if not ready:
                return False

            return True
        return False
