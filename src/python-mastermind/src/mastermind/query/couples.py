from mastermind.query import Query, LazyDataObject
from mastermind.query.groups import Group


class CouplesQuery(Query):
    def __init__(self, client):
        super(CouplesQuery, self).__init__(client)
        self._filter = {}

    def __getitem__(self, key):
        return Couple(self.client, key)

    # def next_group_ids(self, count=1):
    #     """Fetch some free group ids.

    #     Elliptics groups are identified by integer group ids. Mastermind provides
    #     a sequence of increasing group ids for assigning to new groups added to storage.

    #     Args:
    #       count: number of group ids to fetch.
    #     """
    #     return self.client.request('get_next_group_number', count)

    def __iter__(self):
        groups = self.client.request('get_couples_list', [self._filter])
        for c_data in groups:
            cq = Couple(self.client, CoupleDataObject._raw_id(c_data))
            cq._set_raw_data(c_data)
            yield cq


class CoupleDataObject(LazyDataObject):
    def _fetch_data(self):
        return self.client.request('get_couple_info_by_coupleid', self.id)

    @staticmethod
    def _raw_id(raw_data):
        return raw_data['id']

    @property
    @LazyDataObject._lazy_load
    def status(self):
        """Current status of couple.

        Possible values:
          'INIT' - newly created couple or any of the groups has INIT status;
          'OK' - couple is up and ready for write requests;
          'FULL' - couple is up but has no available space for write requests;
          'FROZEN' - couple was frozen and should not participate in write requests;
          'RO' - any of couple's groups are in read-only state or migrating;
          'SERVICE_ACTIVE' - some of couple's groups are being processed in move or restore job,
            job is executing;
          'SERVICE_STALLED' - some of couple's groups are being processed in move or restore job,
            but job is in pending state and requires attention;
          'BROKEN' - couple's configuration is invalid, text description is available through
            status_text attribute;
          'BAD' - represents error state, text description is available through
            status_text attribute;
        """
        return self._data['couple_status']

    @property
    @LazyDataObject._lazy_load
    def status_text(self):
        """Human-readable and clarifying version of status.
        """
        return self._data['couple_status_text']

    @property
    @LazyDataObject._lazy_load
    def as_tuple(self):
        """Tuple of coupled groups' ids.
        """
        return self._data['tuple']

    @property
    @LazyDataObject._lazy_load
    def groups(self):
        """Coupled groups.
        """
        return self._data['groups']

    def _preprocess_raw_data(self, data):
        groups = []
        for g_data in data['groups'][:]:
            groups.append(Group(self.client, Group._raw_id(g_data)))
        data['groups'] = groups
        return data


class CoupleQuery(Query):
    def __init__(self, client, id):
        super(CoupleQuery, self).__init__(client)
        self.id = id

    # @property
    # def meta(self):
    #     """Reads metakey for group.

    #     Returns:
    #       Group metakey, already unpacked.
    #     """
    #     return self.client.request('get_group_meta', [self.id, None, True])['data']

    # def move(self, uncoupled_groups=None, force=False):
    #     """Create group move job.

    #     Job will move group's node backend to uncoupled group's node backend.
    #     Uncoupled group will be replaces, source group node backend will be disabled.

    #     Args:
    #       uncoupled_groups: list of uncoupled group that should be merged together
    #         and replaced by source group.
    #       force: cancel all pending jobs of low priority (e.g. recover-dc and defragmentation).

    #     Returns:
    #       A json of created job (or a dict with a single error key and value).
    #     """
    #     uncoupled_groups = [GroupQuery._object(self.client, g) for g in uncoupled_groups or []]
    #     return self.client.request('move_group',
    #                                [self.id,
    #                                 {'uncoupled_groups': [g.id for g in uncoupled_groups]},
    #                                 force])


class Couple(CoupleQuery, CoupleDataObject):
    pass
