from mastermind.query import Query, LazyDataObject


class NodeBackendQuery(Query):
    pass


class NodeBackendDataObject(LazyDataObject):

    @staticmethod
    def _raw_id(raw_data):
        return raw_data['id']

    @property
    @LazyDataObject._lazy_load
    def host(self):
        """ Node backend's host IP address
        """
        return self._data['host']

    @property
    @LazyDataObject._lazy_load
    def hostname(self):
        """ Node backend's host FQDN
        """
        return self._data['hostname']

    @property
    @LazyDataObject._lazy_load
    def port(self):
        """ Node backend's port
        """
        return self._data['port']

    @property
    @LazyDataObject._lazy_load
    def family(self):
        """ Node backend's family
        """
        return self._data['family']

    @property
    @LazyDataObject._lazy_load
    def backend_id(self):
        """ Node backend's id
        """
        return self._data['backend_id']

    @property
    @LazyDataObject._lazy_load
    def status(self):
        return self._data['status']

    @property
    @LazyDataObject._lazy_load
    def status_text(self):
        return self._data['status_text']

    @property
    @LazyDataObject._lazy_load
    def path(self):
        """ Node backend's root filesystem path
        """
        return self._data['path']

    # TODO: backward-compatibility with dictionary object,
    # remove when all clients use 'node_backends' array as array of
    # 'NodeBackend' objects
    def __getitem__(self, key):
        return self._data[key]


class NodeBackend(NodeBackendQuery, NodeBackendDataObject):
    def __init__(self, id, client):
        super(NodeBackend, self).__init__(client)
        self.id = id

    def __repr__(self):
        return '<Node backend {}: status {} ({})>'.format(
            self.id,
            self.status,
            self.status_text
        )
