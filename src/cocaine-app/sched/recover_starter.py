import logging
import time

import jobs
from mastermind_core.config import config
import storage


logger = logging.getLogger('mm.sched.recover')


class RecoveryStarter(object):

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.params = config.get('scheduler', {}).get('recover_dc', {})
        period_val = self.params.get("recover_dc_period", 60 * 15)  # 15 minutes default
        scheduler.register_periodic_func(self._do_recover_dc,
                                         period_val=period_val,
                                         starter_name="recover_dc")

    def _do_recover_dc(self):

        weight_groupsets = self._get_groupsets_and_weights_needing_recovery()

        jobs_param = []

        for weight_groupset_pair in weight_groupsets:
            jobs_param.append({'couple': weight_groupset_pair[1]})

        created_jobs = self.scheduler.create_jobs(jobs.JobTypes.TYPE_RECOVER_DC_JOB, jobs_param, self.params)

        logger.info('Successfully created {0} recover dc jobs'.format(len(created_jobs)))

    def _get_groupsets_and_weights_needing_recovery(self):
        """
        Analyzing all groupsets in order to find ones that need recover more then others
        :return: a list of tuples(weight, groupset_id)
        """

        ts = int(time.time())
        groupset_weights = []

        # by default one key loss is equal to one day without recovery
        keys_cf = self.params.get('keys_cf', 86400)
        ts_cf = self.params.get('timestamp_cf', 1)
        def weight(keys_diff, ts_diff):
            return keys_diff * keys_cf + ts_diff * ts_cf

        min_keys_loss = self.params.get('min_key_loss', 1)
        history_data = self.scheduler.get_history()

        # Use groupsets not couples, since recovery works for 3 replicas not for 1.5 (LRC_
        for groupset in storage.replicas_groupsets.keys():

            if groupset.status not in storage.GOOD_STATUSES:
                continue
            g_diff = groupset.keys_diff
            if g_diff < min_keys_loss:
                continue
            groupset_str = str(groupset)
            if groupset_str not in history_data:
                # a new couple?
                continue
            # store a list instead of dict, since then sorting by first value of tuple may be done without calling
            # extra function what is much more effective. See Schwartzian transform
            groupset_weights.append((weight(g_diff, ts - history_data[groupset_str]['recover_ts']), groupset_str))

        groupset_weights.sort(reverse=True)
        return groupset_weights

