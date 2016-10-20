import logging

import pymongo

from mastermind_core.config import config
from mastermind_core.meta_db import meta_db


logger = logging.getLogger('mm.max_group')


class MaxGroupManager(object):

    MAX_GROUP = 'max_group'
    UPDATE_ATTEMPTS = 3

    def __init__(self):
        try:
            db_name = config['metadata']['groups']['db']
        except KeyError:
            raise RuntimeError('Groups metadb is not set up (metadata.groups.db key)')
        self._collection = meta_db[db_name]['max_group']

    def _increase_max_group_id(self, delta):
        # NOTE: 'query' parameter is required for find_and_modify command when 'upsert' is True
        return self._collection.find_and_modify(
            query={self.MAX_GROUP: {'$exists': True}},
            update={
                '$inc': {self.MAX_GROUP: delta}
            },
            upsert=True,
            sort=[(self.MAX_GROUP, pymongo.DESCENDING)],
            new=True,
        )

    def update_max_group_id(self, group_id):
        """ Set cluster's max group id to 'group_id'
        """
        cur_group_id = 0

        result = self._collection.find_one(
            {self.MAX_GROUP: {'$exists': True}},
            sort=[(self.MAX_GROUP, pymongo.DESCENDING)],
        )

        if result:
            cur_group_id = result[self.MAX_GROUP]

        while cur_group_id < group_id:

            logger.info('Storage max group: updating to {}'.format(group_id))

            # NOTE: $inc is used here since it provides atomicity, therefore we have a guarantee
            # that the value will actually change in case of several concurrent clients
            #
            # NOTE: in case when several concurrent clients are updating 'max_group' several
            # increments can be executed at the same time, and stored 'max_group' value can become
            # greater than desired 'group_id'. This effect is considered acceptable
            result = self._increase_max_group_id(group_id - cur_group_id)
            cur_group_id = result[self.MAX_GROUP]

            logger.info('Storage max group: updated, current value is {}'.format(cur_group_id))

    def reserve_group_ids(self, count):
        """ Reserve 'count' group ids

        This is implemented by increasing cluster's max group id by the value of 'count'.
        """
        result = self._increase_max_group_id(count)
        start_group_id = result[self.MAX_GROUP] - count + 1
        return range(start_group_id, start_group_id + count)


max_group_manager = MaxGroupManager()
