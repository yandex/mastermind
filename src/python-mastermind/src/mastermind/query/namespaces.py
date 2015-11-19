import copy

from mastermind.query import Query, LazyDataObject
from mastermind.query.couples import CouplesQuery, Couple


class NamespacesQuery(Query):

    DEFAULT_FILTER = {}

    def __init__(self, client, filter=None):
        super(NamespacesQuery, self).__init__(client)
        self._filter = filter or self.DEFAULT_FILTER

    def __getitem__(self, key):
        return Namespace(key, self.client)

    def __iter__(self):
        namespaces = self.client.request('get_namespaces_list', [self._filter])
        for ns_data in namespaces:
            nsq = Namespace(NamespaceDataObject._raw_id(ns_data), self.client)
            del ns_data['namespace']
            nsq._set_raw_data(ns_data)
            yield nsq

    def __contains__(self, key):
        namespaces = self.client.request('get_namespaces_list', [self._filter])
        for ns_data in namespaces:
            nsq = Namespace(NamespaceDataObject._raw_id(ns_data), self.client)
            if nsq == key:
                return True
        return False

    def __delitem__(self, namespace):
        if self._filter != self.DEFAULT_FILTER:
            raise ValueError('Filter object does not support delete operation on non-empty filter')
        self.client.request('namespace_delete', [namespace])

    def filter(self, **kwargs):
        """Filter namespaces list.

        Args:
          deleted: get all namespaces (None), alive namespaces (False) or
            deleted ones (True).

        Returns:
          New namespaces query object with selected filter parameters.
        """
        updated_filter = copy.copy(self._filter)
        if 'deleted' in kwargs:
            updated_filter['deleted'] = kwargs['deleted']
        return NamespacesQuery(self.client, filter=updated_filter)

    def setup(self,
              namespace,
              static_couple=None,
              groups_count=None,
              success_copies=None,
              auth_key_write=None, auth_key_read=None,
              sign_token=None, sign_path_prefix=None,
              min_units=None, add_units=None,
              redirect_content_length_threshold=None,
              redirect_expire_time=None,
              redirect_query_args=None,
              multipart_content_length_threshold=None,
              select_couple_to_upload=None,
              reserved_space_percentage=None,
              check_for_update=None,
              custom_expiration_time=None):
        """Performs initial namespace setup.

        Args:
          namespace:
            id of namespace that is being set up
          overwrite: set namespace settings from scratch using only currently
            provided options
          static_couple: static couple's string identificator. Used when namespace
            does not store metainformation and therefore group balancing is not
            applicable.
          groups_count: default number of groups per couple.
          success_copies: client's success copy politics !!!!! (any|quorum|all). !!!!!
          auth_key_write: proxy auth-key for writing to namespace.
          auth_key_read: proxy auth-key for reading from namespace.
          sign_token: signature token
          sign_path_prefix: signature path prefix
          min_units: minimal number of couples available for write operations
            when namespace is considered available
          add_unit: number of additional couples with positive weights
            that mastermind will add to namespace group weights if available
          redirect_content_length_threshold: content length threshold for
            proxy to return direct urls instead of balancer urls
          redirect_expire_time: period of time for which redirect url
                is considered valid
          redirect_query_args: query arguments that should be included
            in redirect link to storage when it is being formed by proxy
          multipart_content_length_threshold: this flag enables multipart upload for
            requests with content length less than threshold if this flag is True
          select_couple_to_upload: this flag allows client to manually select a couple
            to write key to
          reserved_space_percentage: percentage of effective space that
            will be reserved for future updates, couple will be closed when
            free effective space percentage is less than or equal to reserved
            space percentage
          check_for_update: this flag allows to upload the key to namespace only
            if does not exists already
          custom_expiration_time: allows namespace to use expire-time argument
            for signing url

        Returns:
          Namespace object representing created namespace.
        """
        if namespace in self and not self[namespace].deleted:
            raise ValueError('Namespace {} already exists'.format(namespace))

        settings = {}

        groups_count = int(groups_count) if groups_count else 0

        if not success_copies:
            raise ValueError('success_copies is required')
        settings['success-copies-num'] = success_copies

        static_couple = [int(g) for g in static_couple.split(':')] if static_couple else None

        if (not static_couple and not groups_count):
            raise ValueError('either groups_count or static_couple is required')

        if static_couple:
            settings['static-couple'] = static_couple
        elif groups_count:
            settings['groups-count'] = groups_count
        if sign_token:
            settings.setdefault('signature', {})['token'] = sign_token
        if sign_path_prefix:
            settings.setdefault('signature', {})['path_prefix'] = sign_path_prefix
        if auth_key_read:
            settings.setdefault('auth-keys', {})['read'] = auth_key_read
        if auth_key_write:
            settings.setdefault('auth-keys', {})['write'] = auth_key_write

        if min_units:
            settings['min-units'] = min_units
        if add_units:
            settings['add-units'] = add_units
        if reserved_space_percentage:
            settings['reserved-space-percentage'] = reserved_space_percentage

        redirect = {}
        if redirect_content_length_threshold:
            redirect['content-length-threshold'] = int(redirect_content_length_threshold)
        if redirect_expire_time:
            redirect['expire-time'] = int(redirect_expire_time)
        if redirect_query_args:
            redirect['query-args'] = redirect_query_args

        if redirect:
            settings['redirect'] = redirect

        features = {}
        if multipart_content_length_threshold:
            features['multipart'] = {
                'content-length-threshold': int(multipart_content_length_threshold)
            }
        if select_couple_to_upload == 'true':
            features['select-couple-to-upload'] = True
        if custom_expiration_time:
            features['custom-expiration-time'] = custom_expiration_time != '0'

        if features:
            settings['features'] = features

        if check_for_update:
            settings['check-for-update'] = check_for_update != '0'

        ns_data = self.client.request('namespace_setup', [namespace, True, settings, {}])

        ns = Namespace(namespace, self.client)
        ns._set_raw_data(ns_data)

        return ns


class NamespaceDataObject(LazyDataObject):
    class Settings(object):
        def __init__(self, client, namespace, settings, levels=None):
            self._client = client
            self._namespace = namespace
            self._settings = settings
            self._levels = levels or []

        def __getitem__(self, key):
            return NamespaceDataObject.Settings(
                self._client, self._namespace, self._settings[key], levels=self._levels + [key])

        def __setitem__(self, key, value):
            settings = {key: value}
            for level in reversed(self._levels):
                settings = {level: settings}
            self._client.request('namespace_setup', [self._namespace.id, False, settings, {}])
            self._namespace._expire()

        def __repr__(self):
            return repr(self._settings)

        def __str__(self):
            return str(self._settings)

        def __contains__(self, key):
            return key in self._settings

        def __len__(self):
            return len(self._settings)

        def __eq__(self, other):
            if isinstance(other, NamespaceDataObject.Settings):
                return self._settings == other._settings
            return self._settings == other

        def __ne__(self, other):
            return not self == other

        def keys(self):
            return self._settings.keys()

        def values(self):
            return [v for _, v in self.items()]

        def items(self):
            return [(k,
                     NamespaceDataObject.Settings(
                         self._client, self._namespace, v,
                         levels=self._levels + [k]))
                    for k, v in self._settings.iteritems()]

        def iterkeys(self):
            return self._settings.iterkeys()

        def itervalues(self):
            for _, v in self.iteritems():
                yield v

        def iteritems(self):
            for k, v in self._settings.iteritems():
                yield k, NamespaceDataObject.Settings(
                    self._client, self._namespace, v,
                    levels=self._levels + [k])

        def dict(self):
            return self._settings

    def _fetch_data(self):
        return self.client.request('get_namespace_settings', [self.id])

    @staticmethod
    def _raw_id(raw_data):
        return raw_data['namespace']

    @property
    @LazyDataObject._lazy_load
    def settings(self):
        return NamespaceDataObject.Settings(self.client, self, self._data)

    @settings.setter
    def settings(self, new_settings):
        self.client.request('namespace_setup', [self.id, True, new_settings, {}])
        self._expire()

    def update(self, new_settings):
        self.client.request('namespace_setup', [self.id, False, new_settings, {}])
        self._expire()

    @property
    @LazyDataObject._lazy_load
    def deleted(self):
        """Check if namespace is deleted.
        """
        return self._data['__service'].get('is_deleted', False)


class NamespaceQuery(Query):
    @Query.not_idempotent
    def build_couples(self, couple_size, init_state,
                      couples=1, groups=None, ignore_space=False, dry_run=False,
                      attempts=None, timeout=None):
        """
        Builds a number of couples to extend a namespace.

        Args:
          couple_size:
            a number of groups to couple together.
          init_state:
            couple init state (should take one of COUPLE_INIT_*_STATE values).

        KwArgs:
          couples:
            number of couples that mastermind will try to create.
          namespace:
            all created couples will belong to provided namespace.
          groups:
            iterable of sets of mandatory groups that should be coupled together:
            Example: ((42, 69), # groups 42 and 69 will be included in the first created couple,
                      (128))    # group 128 will be included in the second one, and so on.
          ignore_space:
            if this flag is set to True mastermind will couple only the groups
            having equal total space
          dry_run:
            build couple in dry-run mode.
            Mastermind will not write corresponding metakeys to selected groups,
            which effectively means that couples will not be created.

        Returns:
          List of created couples.
        """

        params = [couple_size, couples, {'namespace': self.id,
                                         'match_group_space': not ignore_space,
                                         'init_state': init_state,
                                         'dry_run': dry_run,
                                         'mandatory_groups': groups or []}]
        created_couples = []
        for couple_data in self.client.request('build_couples', params,
                                               attempts=attempts, timeout=timeout):
            if isinstance(couple_data, basestring):
                created_couples.append(couple_data)
                continue
            c = Couple(Couple._raw_id(couple_data), self.client)
            c._set_raw_data(couple_data)
            created_couples.append(c)

        return CouplesBuildResult(created_couples)

    @property
    def couples(self):
        return CouplesQuery(self.client).filter(namespace=self)


class Namespace(NamespaceQuery, NamespaceDataObject):
    def __init__(self, id, client=None):
        super(Namespace, self).__init__(client)
        self.id = id

    def __eq__(self, o):
        if isinstance(o, basestring):
            return self.id == o
        elif isinstance(o, Namespace):
            return self.id == o.id
        return False

    def __repr__(self):
        return '<Namespace {}{}>'.format(self.id, ' [DELETED]' if self.deleted else '')


class CouplesBuildResult(object):
    def __init__(self, result):
        self.result = result

    def __iter__(self):
        for r in self.result:
            yield r

    def filter(self, success=None):
        """
        Filters build couple request results.

        By default build couple request result is an iterable which contains
        successfully created couples as well as errors that happened during a certain
        couple creation. This method helps to separate successfully created couples from
        couple creation errors.

        Args:
            success:
                boolean flag that should be set to True to retrieve successfully created couples
                and False for fetching only errors during couple creation.

        Returns:
            an iterable of couple build request results, filtered if required.
        """
        def filter_records(r):
            if success is None:
                return True
            return isinstance(r, Couple) == success
        return CouplesBuildResult(filter(filter_records, self.result))
