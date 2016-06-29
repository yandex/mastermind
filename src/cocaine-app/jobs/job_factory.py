from job_types import JobTypes
from move import MoveJob
from recover_dc import RecoverDcJob
from couple_defrag import CoupleDefragJob
from restore_group import RestoreGroupJob
from make_lrc_groups import MakeLrcGroupsJob
from add_lrc_groupset import AddLrcGroupsetJob
from convert_to_lrc_groupset import ConvertToLrcGroupsetJob


class JobFactory(object):

    @staticmethod
    def make_job_type(job_type):
        if job_type == JobTypes.TYPE_MOVE_JOB:
            return MoveJob
        elif job_type == JobTypes.TYPE_RECOVER_DC_JOB:
            return RecoverDcJob
        elif job_type == JobTypes.TYPE_COUPLE_DEFRAG_JOB:
            return CoupleDefragJob
        elif job_type == JobTypes.TYPE_RESTORE_GROUP_JOB:
            return RestoreGroupJob
        elif job_type == JobTypes.TYPE_MAKE_LRC_GROUPS_JOB:
            return MakeLrcGroupsJob
        elif job_type == JobTypes.TYPE_ADD_LRC_GROUPSET_JOB:
            return AddLrcGroupsetJob
        elif job_type == JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB:
            return ConvertToLrcGroupsetJob
        raise ValueError('Unknown job type: {}'.format(job_type))

    @staticmethod
    def make_job(data):
        JobType = JobFactory.make_job_type(data.get('type'))
        return JobType.from_data(data)
