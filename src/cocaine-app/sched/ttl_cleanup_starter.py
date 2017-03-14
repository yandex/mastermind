import logging
import time
import datetime

from infrastructure import infrastructure
import jobs
from mastermind_core.config import config
import storage
from yt_worker import YqlWrapper


logger = logging.getLogger('mm.sched.ttl_cleanup')


class TtlCleanupStarter(object):
    def __init__(self, scheduler):
        scheduler.register_periodic_func(self._do_ttl_cleanup,
                                         period_default=60*60*24,  # once per day
                                         starter_name="ttl_cleanup")
        self.params = config.get('scheduler', {}).get('ttl_cleanup', {})
        self.scheduler = scheduler

    def _get_from_yt_groups_with_expired_data(self):
        """
        Extract statistics from YT logs
        :return: list of couples ids with expired records volume above the specified
        """
        try:
            # Configure parameters for work with YT
            yt_cluster = self.params.get('yt_cluster', "")
            yt_token = self.params.get('yt_token', "")
            yt_attempts = self.params.get('yt_attempts', 3)
            yt_delay = self.params.get('yt_delay', 10)

            yt_wrapper = YqlWrapper(cluster=yt_cluster, token=yt_token, attempts=yt_attempts, delay=yt_delay)

            aggregation_table = self.params.get('aggregation_table', "")
            base_table = self.params.get('tskv_log_table', "")
            expired_threshold = self.params.get('ttl_threshold', 10 * float(1024 ** 3))  # 10GB

            yt_wrapper.prepare_aggregate_for_yesterday(base_table, aggregation_table)

            group_id_list = yt_wrapper.request_expired_stat(aggregation_table, expired_threshold)
            logger.info("YT request has completed")
            return group_id_list
        except:
            logger.exception("Work with YQL failed")
            return []

    def _get_idle_group_ids(self, days_of_idle):
        """
        Iterates all over couples. Find couples where ttl_cleanup hasn't run for more than 'days of idle'
        :param days_of_idle: how long the group could be idle
        :return: list of groups[0] from couples
        """

        idle_group_ids = []

        # the epoch time when executed jobs are considered meaningful
        idleness_threshold = time.time() - datetime.timedelta(days=days_of_idle).total_seconds()

        couples_data = self.scheduler.get_history()
        # couples data is not sorted, so we need to iterate through it all.
        # But it may be faster then creation of a sorted representation

        for couple_id, couple_data in couples_data.iteritems():

            # if couple_data doesn't contain cleanup_ts field then cleanup_ts has never been run on this couple
            # and None < idleness_threshold
            ts = couple_data.get('ttl_cleanup_ts', None)
            if ts > idleness_threshold:
                continue

            # couple has format "gr0:gr1:...:grN". We are interested only in the group #0
            idle_group_ids.append(int(couple_id.split(":")[0]))

        return idle_group_ids

    def _do_ttl_cleanup(self):
        logger.info("Run ttl cleanup")

        job_params = []

        allowed_idleness_period = self.params.get('max_idle_days', 270)

        # get couples where ttl_cleanup wasn't run for long time (or never)
        time_group_list = self._get_idle_groups(days_of_idle=allowed_idleness_period)

        # get information from mds-proxy YT logs
        yt_group_list = self._get_yt_stat()

        # remove dups
        gid_list = set(yt_group_list + time_group_list)

        for gid in gid_list:

            iter_group = gid  # in tskv couple id is actually group[0] from couple id
            if iter_group not in storage.groups:
                logger.error("Not valid group is extracted from aggregation log {}".format(iter_group))
                continue
            iter_group = storage.groups[iter_group]
            if not iter_group.couple:
                logger.error("Iter group is uncoupled {}".format(str(iter_group)))
                continue

            job_params.append(
                {
                    'iter_group': iter_group.group_id,
                    'couple': str(iter_group.couple),
                    'namespace': iter_group.couple.namespace.id,
                    'batch_size': None,  # get from config
                    'attempts': None,  # get from config
                    'nproc': None,  # get from config
                    'wait_timeout': None,  # get from config
                    'dry_run': False
                }
            )

        jobs_created = self.scheduler.create_jobs(jobs.JobTypes.TYPE_TTL_CLEANUP_JOB, job_params, self.params)
        logger.info("Created {} ttl cleanup jobs out of {} proposed".format(len(jobs_created), len(job_params)))
