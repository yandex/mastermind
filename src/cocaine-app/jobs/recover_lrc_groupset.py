import logging
import random

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


class RecoverLrcGroupsetJob(Job):
    PARAMS = (
        'lrc_groupset',
        'couple',  # stored to use for locks
        'groups',  # stored to use for locks
        'resources',
    )

    def __init__(self, **kwargs):
        super(RecoverLrcGroupsetJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RECOVER_LRC_GROUPSET_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(RecoverLrcGroupsetJob, cls).new(*args, **kwargs)
        try:
            lrc_groupset = storage.lrc_groupsets[job.lrc_groupset]
            job.groups = [group.group_id for group in lrc_groupset.groups]
            job.couple = str(lrc_groupset.couple)
        except Exception:
            job.release_locks()
            raise

        return job

    def _set_resources(self):
        self.resources = {}

    def create_tasks(self, processor):

        self.tasks.extend(
            self._lrc_recover_tasks()
        )

        if LRC_RESTORE_CFG.get('external_storage_validation', False):
            self.tasks.extend(
                self._lrc_validate_task(processor)
            )

    def _lrc_recover_tasks(self):

        job_tasks = []

        lrc_groupset = storage.lrc_groupsets[self.lrc_groupset]

        scheme = storage.Lrc.make_scheme(lrc_groupset.scheme)

        for shard_group_idxs in scheme.INDEX_SHARD_INDICES:
            shard_groups = [
                lrc_groupset.groups[index]
                for index in shard_group_idxs
            ]

            exec_group = random.choice(shard_groups)

            recover_lrc_index_shard_cmd = infrastructure._recover_lrc_index_shard_cmd(
                lrc_groupset=lrc_groupset,
                broken_group=exec_group,
                lrc_reserve_group=exec_group,  # recovery is performed in-place
                json_stats=True,
                trace_id=self.id[:16],
            )

            nb = infrastructure.get_backend_by_group_id(exec_group.group_id)

            job_tasks.append(
                tasks.RecoverGroupDcTask.new(
                    self,
                    group=exec_group.group_id,
                    host=nb.node.host.addr,
                    cmd=recover_lrc_index_shard_cmd,
                    params={
                        'node_backend': self.node_backend(
                            host=nb.node.host.addr,
                            port=nb.node.port,
                            backend_id=nb.backend_id,
                        ),
                        'group': str(exec_group.group_id),
                    }
                )
            )

        exec_group = random.choice(lrc_groupset.groups)
        nb = infrastructure.get_backend_by_group_id(exec_group.group_id)

        recover_cmd = infrastructure._lrc_recovery_cmd(
            lrc_groupset=lrc_groupset,
            trace_id=self.id[:16],
            json_stats=True,
        )

        job_tasks.append(
            tasks.LrcRecoveryTask.new(
                self,
                group=exec_group.group_id,
                host=nb.node.host.addr,
                cmd=recover_cmd,
                params={
                    'node_backend': self.node_backend(
                        host=nb.node.host.addr,
                        port=nb.node.port,
                        backend_id=nb.backend_id,
                    ),
                    'group': str(exec_group.group_id),
                }
            )
        )

        return job_tasks

    def _lrc_validate_task(self, processor):
        job_tasks = []

        lrc_groupset = storage.lrc_groupsets[self.lrc_groupset]
        exec_group = random.choice(lrc_groupset.groups)
        nb = infrastructure.get_backend_by_group_id(exec_group.group_id)
        couple = lrc_groupset.couple

        mappings = processor.external_storage_meta.mapping_list(
            couple=[couple.couple_id]
        )

        if len(mappings) == 0:
            logger.debug(
                'Failed to find external storage mapping for couple {}, couple was not converted '
                'from external storage'.format(
                    couple
                )
            )
            return []

        mapping = mappings[0]

        dst_groups = []
        for couple_id in mapping.couples:
            mapped_couple = storage.couples[str(couple_id)]
            dst_groups.append(mapped_couple.lrc822v1_groupset.groups)

        validate_cmd = inventory.make_external_storage_validate_command(
            dst_groups=dst_groups,
            groupset_type=storage.GROUPSET_LRC,
            groupset_settings=lrc_groupset.groupset_settings,
            src_storage=mapping.external_storage,
            src_storage_options=mapping.external_storage_options,
            trace_id=self.id[:16],
        )

        job_tasks.append(
            tasks.ExternalStorageTask.new(
                self,
                host=nb.node.host.addr,
                cmd=validate_cmd,
            )
        )

        return job_tasks

    @property
    def _involved_groups(self):
        lrc_groupset = storage.lrc_groupsets[self.lrc_groupset]
        return [group.group_id for group in lrc_groupset.groups]

    @property
    def _involved_couples(self):
        lrc_groupset = storage.lrc_groupsets[self.lrc_groupset]
        return [str(lrc_groupset.couple)]
