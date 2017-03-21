from job_types import JobTypes
from move import MoveJob
from recover_dc import RecoverDcJob
from couple_defrag import CoupleDefragJob
from restore_group import RestoreGroupJob
from restore_lrc_group import RestoreLrcGroupJob
from recover_lrc_groupset import RecoverLrcGroupsetJob
from restore_uncoupled_lrc_group import RestoreUncoupledLrcGroupJob
from backend_cleanup import BackendCleanupJob
from make_lrc_groups import MakeLrcGroupsJob
from make_lrc_reserved_groups import MakeLrcReservedGroupsJob
from add_lrc_groupset import AddLrcGroupsetJob
from convert_to_lrc_groupset import ConvertToLrcGroupsetJob
from ttl_cleanup import TtlCleanupJob
from backend_manager import BackendManagerJob


class JobFactory(object):
    JOB_TYPES = {
        JobTypes.TYPE_MOVE_JOB: MoveJob,
        JobTypes.TYPE_RECOVER_DC_JOB: RecoverDcJob,
        JobTypes.TYPE_COUPLE_DEFRAG_JOB: CoupleDefragJob,
        JobTypes.TYPE_RESTORE_GROUP_JOB: RestoreGroupJob,
        JobTypes.TYPE_RESTORE_LRC_GROUP_JOB: RestoreLrcGroupJob,
        JobTypes.TYPE_RECOVER_LRC_GROUPSET_JOB: RecoverLrcGroupsetJob,
        JobTypes.TYPE_RESTORE_UNCOUPLED_LRC_GROUP_JOB: RestoreUncoupledLrcGroupJob,
        JobTypes.TYPE_MAKE_LRC_GROUPS_JOB: MakeLrcGroupsJob,
        JobTypes.TYPE_ADD_LRC_GROUPSET_JOB: AddLrcGroupsetJob,
        JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB: ConvertToLrcGroupsetJob,
        JobTypes.TYPE_TTL_CLEANUP_JOB: TtlCleanupJob,
        JobTypes.TYPE_BACKEND_CLEANUP_JOB: BackendCleanupJob,
        JobTypes.TYPE_BACKEND_MANAGER_JOB: BackendManagerJob,
        JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB: MakeLrcReservedGroupsJob,
    }

    @staticmethod
    def make_job_type(job_type):
        if job_type not in JobFactory.JOB_TYPES:
            raise ValueError('Unknown job type: {}'.format(job_type))
        return JobFactory.JOB_TYPES[job_type]

    @staticmethod
    def make_job(data):
        JobType = JobFactory.make_job_type(data.get('type'))
        return JobType.from_data(data)
