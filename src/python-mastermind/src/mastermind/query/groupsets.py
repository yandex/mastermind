import copy

from mastermind import query
from mastermind.query import Query, LazyDataObject
# imported as a module because of cross-dependencies with 'groupset' module
import mastermind.query.groups


class GroupsetDataObject(LazyDataObject):
    def _fetch_data(self):
        return self.client.request('get_groupset_by_id', self.id)

    @staticmethod
    def _raw_id(raw_data):
        return raw_data['id']

    @property
    @LazyDataObject._lazy_load
    def status(self):
        """Current status of couple.

        Possible values:
          'INIT' - newly created groupset or any of the groups has INIT status;
          'OK' - groupset is up and ready for write requests;
          'FULL' - groupset is up but has no available space for write requests;
          'FROZEN' - groupset was frozen and should not participate in write requests;
          'RO' - any of groupset's groups are in read-only state or migrating;
          'SERVICE_ACTIVE' - some of groupset's groups are being processed in move or restore job,
            job is executing;
          'SERVICE_STALLED' - some of groupset's groups are being processed in move or restore job,
            but job is in pending state and requires attention;
          'ARCHIVED' - groupset's couple is in archived state, does not accept new data and
            has lrc groupset with copy of groupset's data;
          'BROKEN' - groupset's configuration is invalid, text description is available through
            status_text attribute;
          'BAD' - represents error state, text description is available through
            status_text attribute;
        """
        return self._data['status']

    @property
    @LazyDataObject._lazy_load
    def status_text(self):
        """Human-readable and clarifying version of status.
        """
        return self._data['status_text']

    @property
    @LazyDataObject._lazy_load
    def group_ids(self):
        """ List of ids of groups participating in groupset.
        """
        return self._data['group_ids']

    @property
    @LazyDataObject._lazy_load
    def groups(self):
        """ Groups in a groupset
        """
        return self._data['groups']

    @property
    @LazyDataObject._lazy_load
    def type(self):
        """ Groupset type
        """
        return self._data['type']

    def _preprocess_raw_data(self, data):
        groups = []
        for g_data in data['groups']:
            groups.append(mastermind.query.groups.Group.from_data(g_data, self.client))
        data['groups'] = groups
        return data

    def serialize(self):
        data = super(GroupsetDataObject, self).serialize()
        groups = [group.serialize() for group in data['groups']]
        data['groups'] = groups
        return data


GOOD_STATUSES = set(['OK', 'FULL', 'FROZEN'])


class GroupsetQuery(Query):
    pass


class Groupset(GroupsetQuery, GroupsetDataObject):
    def __init__(self, id, client=None):
        super(Groupset, self).__init__(client)
        self.id = id

    def __repr__(self):
        return '<Groupset {}: status {} ({})>'.format(self.id, self.status, self.status_text)


class GroupsetsQuery(Query):
    def __init__(self, client, filter=None):
        super(GroupsetsQuery, self).__init__(client)
        self._filter = filter or {}

    def __getitem__(self, key):
        return Groupset(key, self.client)

    def __iter__(self):
        groupsets = self.client.request('get_groupsets_list', {'filter': self._filter})
        for gs_data in groupsets:
            gs = Groupset(GroupsetDataObject._raw_id(gs_data), self.client)
            gs._set_raw_data(gs_data)
            yield gs

    def __contains__(self, key):
        # TODO: this should be implemented better than in CouplesQuery object
        raise NotImplemented

    @property
    def replicas(self):
        """ Get replicas groupsets
        """
        return self.filter(type='replicas')

    @property
    def lrc(self):
        """ Get lrc groupsets
        """
        return self.filter(type='lrc')

    def filter(self, **kwargs):
        """Filter groupsets list.

        Keyword args:
          namespace: get groupsets belonging to a certain namespace.
          state: mostly the same as groupsets status, but one state can actually
            combine several statuses. Represents groupset state from admin's point of view.
            States to groupset statuses:
            good: OK
            full: FULL
            frozen: FROZEN
            bad: INIT, BAD
            broken: BROKEN
            service-active: SERVICE_ACTIVE
            service-stalled: SERVICE_STALLED
            archived: ARCHIVED
          type: specific groupset type (by default there is no type restrictions):
            replicas: regular replicas groupsets
            lrc: lrc groupsets

        Returns:
          New groupsets query object with selected filter parameters.
        """
        updated_filter = copy.copy(self._filter)
        if 'namespace' in kwargs:
            # TODO: rename _object method to 'to_object' or something similar
            updated_filter['namespace'] = query.namespaces.Namespace._object(
                kwargs['namespace'],
                self.client
            ).id
        if 'state' in kwargs:
            updated_filter['state'] = kwargs['state']
        if 'type' in kwargs:
            updated_filter['type'] = kwargs['type']
        return GroupsetsQuery(self.client, filter=updated_filter)

    def __delitem__(self, key):
        return Groupset._object(key, self.client).remove()
