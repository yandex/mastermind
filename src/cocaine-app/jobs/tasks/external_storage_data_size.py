import logging

from jobs import TaskTypes, JobBrokenError
from external_storage import ExternalStorageTask
import storage


logger = logging.getLogger('mm.jobs')


class ExternalStorageDataSizeTask(ExternalStorageTask):

    PARAMS = ExternalStorageTask.PARAMS + ('groupset_type', 'mandatory_dcs')

    def __init__(self, job):
        super(ExternalStorageDataSizeTask, self).__init__(job)
        self.type = TaskTypes.TYPE_EXTERNAL_STORAGE_DATA_SIZE

    GROUPSET_SELECT_ATTEMPTS = 3

    def _on_exec_stop(self, processor):
        if self.status == self.STATUS_EXECUTING:

            command_state = processor.minions_monitor.get_minion_cmd_state(self.minion_cmd)

            try:
                data_size = self._data_size(command_state['output'])
            except ValueError as e:
                raise JobBrokenError(str(e))

            if data_size == 0:
                logger.info(
                    'Job {job_id}, task {task_id}: determined data size is 0, converting is not '
                    'required'.format(
                        job_id=self.parent_job.id,
                        task_id=self.id,
                    )
                )
                return

            last_error = None

            for _ in xrange(self.GROUPSET_SELECT_ATTEMPTS):
                total_space = 0
                groupsets = []
                selected_groups = set()

                while total_space < data_size:
                    try:
                        groups = processor._select_groups_for_groupset(
                            type=self.groupset_type,
                            mandatory_dcs=self.mandatory_dcs,
                            skip_groups=selected_groups,
                        )
                    except Exception as e:
                        raise JobBrokenError(str(e))
                    groupsets.append(groups)
                    selected_groups.update(groups)

                    total_space += sum(
                        storage.groups[g_id].effective_space
                        for g_id in groups[:storage.Lrc.Scheme822v1.NUM_DATA_PARTS]
                    )
                    logger.info(
                        'Job {job_id}, task {task_id}: selected groupset {groupset}, accumulated '
                        'total space: {total_space} / {data_size}'.format(
                            job_id=self.parent_job.id,
                            task_id=self.id,
                            groupset=groups,
                            total_space=total_space,
                            data_size=data_size,
                        )
                    )

                logger.info(
                    'Job {job_id}, task {task_id}: performing locks on selected groupsets '
                    '{groupsets}'.format(
                        job_id=self.parent_job.id,
                        task_id=self.id,
                        groupsets=groupsets,
                    )
                )
                self.parent_job.groups = groupsets
                try:
                    self.parent_job._set_resources()
                except Exception:
                    logger.exception('Job {}: failed to set job resources'.format(self.parent_job.id))
                    raise

                try:
                    self.parent_job.perform_locks()
                except Exception as e:
                    last_error = e
                    logger.error(
                        'Job {job_id}, task {task_id}: failed to perform locks: {error}'.format(
                            job_id=self.parent_job.id,
                            task_id=self.id,
                            error=e,
                        )
                    )
                    continue

                # @determine_data_size is set to False to allow parent job to create
                # convert tasks in a standard mode
                self.parent_job.determine_data_size = False

                try:
                    self.parent_job._update_groups(processor.node_info_updater)
                    self.parent_job._ensure_group_types()

                    self.parent_job.create_tasks(processor)

                    # assign data size task back to parent job
                    self.parent_job.tasks.insert(0, self)
                except Exception as e:
                    last_error = e
                    self.parent_job.release_locks()
                    logger.error(
                        'Job {job_id}, task {task_id}: failed to initialize new tasks: {error}'.format(
                            job_id=self.parent_job.id,
                            task_id=self.id,
                            error=e,
                        )
                    )
                    continue
                finally:
                    # set @determine_data_size to its original value
                    self.parent_job.determine_data_size = True

                # groupsets were successfully selected
                break

            else:
                raise RuntimeError('Failed to select groupsets, last error: {}'.format(last_error))

    @staticmethod
    def _data_size(output):
        try:
            data_size = int(output)
        except ValueError:
            raise ValueError('Unexpected storage data size returned from command stdout')

        if data_size < 0:
            raise ValueError('Unexpected storage data size returned from command stdout')

        return data_size
