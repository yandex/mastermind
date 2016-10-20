import datetime
import logging
import time

import pymongo

from mastermind_core.config import config
from mastermind_core.db.mongo.pool import Collection
import storage
from sync import sync_manager
from sync.error import LockAlreadyAcquiredError


logger = logging.getLogger('mm.monitor')


class CoupleFreeEffectiveSpaceMonitor(object):
    """
    Performs monitoring for couple free effective space distribution in a namespace

    To get the picture of how namespace is being filled up in time we need to record
    samples of couples' free effective space distribution periodically.
    The following steps are performed to collect the measurements:
        1) all namespace couples are sorted by free effective space percentage;
        2) sample is formed by:
            a) recording each couple's free effective space percentage if
               namespace has less than <MAX_DATA_POINTS> couples;
            b) selecting couples uniformly and recording its free effective
               space percentage to get <MAX_DATA_POINTS> measurements;
    """

    COUPLE_FREE_EFF_SPACE_DATA = 'statistics/couple_free_eff_space'

    STAT_CFG = config.get('metadata', {}).get('statistics', {})
    CFES_STAT_CFG = STAT_CFG.get('couple_free_effective_space', {})

    RECORD_WRITE_ATTEMPTS = STAT_CFG.get('write_attempts', 3)
    MAX_DATA_POINTS = CFES_STAT_CFG.get('max_data_points', 500)
    DATA_COLLECT_PERIOD = CFES_STAT_CFG.get('collect_period', 3600)

    def __init__(self, db):
        self.collection = Collection(db[config['metadata']['statistics']['db']],
                                     'couple_free_effective_space')

    def collect(self):
        """
        Runs samples collect task

        Samples collection is performed periodically each <DATA_COLLECT_PERIOD> seconds.
        """
        try:
            with sync_manager.lock(
                    CoupleFreeEffectiveSpaceMonitor.COUPLE_FREE_EFF_SPACE_DATA,
                    blocking=False):

                self.__collect_free_effective_space()
        except LockAlreadyAcquiredError:
            logger.info('Couples\' effective free space is already being collected')
            pass

    def get_namespace_samples(self, namespace, limit, skip=0):
        """
        Returns samples of monitor records for a namespace.

        Samples are represented by a dict:
            {
                'total': total number of samples for a namespace;
                'samples': a list of data samples described below;
            }

        A structure of a single data sample:
            {
                'ts': timestamp of a current data sample;
                'data': a list of selected couples' measurements;
            }

        NB: This kind of query can be heavy. It's time complexity is linear in general
        and actual consumed time is increasing when `skip` parameter grows.
        It uses reversed index on keys ('namespace', 'ts') which gives
        best possible time guarantees for our case and allows to fetch
        most recent records in optimal time.
        """
        samples = (
            self.collection
            .find(
                spec={'namespace': namespace},
                fields={
                    '_id': False,
                    'data': True,
                    'ts': True,
                }
            )
            .sort([('ts', pymongo.DESCENDING)])
            .skip(skip)
            .limit(
                max(limit, 0)
            )
        )

        return {
            'total': samples.count(),
            'samples': list(samples),
        }

    def __collect_free_effective_space(self):
        data = []

        for ns in self.__pending_namespaces:
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
    def __pending_namespaces(self):
        namespaces = []
        start_ts = time.time() - self.DATA_COLLECT_PERIOD / 2
        for ns in storage.namespaces:
            res = self.collection.find(
                spec={'namespace': ns.id,
                      'ts': {'$gt': start_ts}},
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
        udt = datetime.datetime.utcnow()
        for ns_id, sample_data in data:
            records.append({
                'namespace': ns_id,
                'ts': ts,
                'data': sample_data,
                'utc_date': udt,
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
