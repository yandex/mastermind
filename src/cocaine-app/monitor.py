import logging
import time

from config import config
from db.mongo.pool import Collection
import storage
from sync import sync_manager
from sync.error import LockAlreadyAcquiredError


logger = logging.getLogger('mm.monitor')


class CoupleFreeEffectiveSpaceMonitor(object):

    COUPLE_FREE_EFF_SPACE_DATA = 'statistics/couple_free_eff_space'
    STAT_CFG = config.get('metadata', {}).get('statistics', {})
    MAX_DATA_POINTS = STAT_CFG.get('max_data_points', 1000)
    RECORD_WRITE_ATTEMPTS = STAT_CFG.get('write_attempts', 3)
    DATA_COLLECT_PERIOD = STAT_CFG.get('collect_period', 300)

    def __init__(self, db):
        self.collection = Collection(db[config['metadata']['statistics']['db']],
                                     'couple_free_effective_space')

    def collect(self):
        try:
            with sync_manager.lock(
                    CoupleFreeEffectiveSpaceMonitor.COUPLE_FREE_EFF_SPACE_DATA,
                    blocking=False):

                self.__collect_free_effective_space()
        except LockAlreadyAcquiredError:
            logger.info('Couples\' effective free space is already being collected')
            pass

    def __collect_free_effective_space(self):
        data = []

        for ns in self.pending_namespaces:
            ns_free_eff_space_pct = []
            for couple in ns.couples:
                if couple.effective_space <= 0:
                    continue
                free_eff_space_pct = float(couple.effective_free_space) / couple.effective_space
                ns_free_eff_space_pct.append(free_eff_space_pct)
            ns_free_eff_space_pct.sort(reverse=True)
            max_src_data_idx = len(ns_free_eff_space_pct) - 1
            data_points = min(len(ns_free_eff_space_pct),
                              CoupleFreeEffectiveSpaceMonitor.MAX_DATA_POINTS)

            sample_data = []
            for step_idx in xrange(1, data_points):
                data_point_idx = int(max_src_data_idx * (float(step_idx) / data_points))
                sample_data.append(ns_free_eff_space_pct[data_point_idx])

            data.append((ns.id, sample_data))

        self.__save_sample_data(data)

    @property
    def pending_namespaces(self):
        namespaces = []
        ts = time.time() - self.DATA_COLLECT_PERIOD
        for ns in storage.namespaces:
            res = self.collection.find(
                spec={'namespace': ns.id,
                      'ts': {'$gt': ts}},
                fields=[],
                limit=1
            )
            if res.count() > 0:
                continue
            namespaces.append(ns)
        return namespaces

    def __save_sample_data(self, data):
        if not data:
            return
        records = []
        ts = int(time.time())
        for ns_id, sample_data in data:
            records.append({
                'namespace': ns_id,
                'ts': ts,
                'data': sample_data,
            })
        total_insert_records = len(data)
        inserted_records = 0
        for i in xrange(self.RECORD_WRITE_ATTEMPTS):
            bulk = self.collection.initialize_ordered_bulk_op()
            for record in records:
                bulk.insert(record)
            try:
                res = bulk.execute()
            except Exception:
                logger.exception('Unexpected exception on collection insertion')
            else:
                inserted_records += res['nInserted']
                logger.debug(
                    'Saving couples\' free effective space records '
                    '(attempt {attempt_num} / {total_attempts_num}): '
                    '{inserted_records_num} / {total_records_num}'.format(
                        attempt_num=i + 1,
                        total_attempts_num=self.RECORD_WRITE_ATTEMPTS,
                        inserted_records_num=inserted_records,
                        total_records_num=total_insert_records
                    )
                )
                for write_error in res['writeErrors']:
                    logger.error(
                        'Error during attempt {attempt_num}: {error} (code {error_code})'.format(
                            attempt_num=i + 1,
                            error=write_error.errmsg,
                            error_code=write_error.code
                        )
                    )
                records = records[inserted_records:]
                if inserted_records >= total_insert_records:
                    break

        if len(records) > 0:
            logger.error(
                'Failed to save couples\' free effective space, saved {} / {}'.format(
                    total_insert_records - len(records),
                    total_insert_records
                )
            )
