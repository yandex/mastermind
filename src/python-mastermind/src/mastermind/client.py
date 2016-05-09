import msgpack

from mastermind.query import groups, namespaces, couples, groupsets, namespaces_states
from mastermind.service import ReconnectableService


class MastermindClient(object):
    """Provides python binding to mastermind cocaine application.

    Args:
      app_name:
        Cocaine application name, defaults to "mastermind2.26".
      **kwargs:
        Parameters for constructing ReconnactableService object which
        is used for cocaine requests.
    """

    DEFAULT_APP_NAME = 'mastermind2.26'

    def __init__(self, app_name=None, **kwargs):
        self.service = ReconnectableService(app_name=app_name or self.DEFAULT_APP_NAME,
                                            **kwargs)

    def request(self, handle, data, attempts=None, timeout=None):
        """Performs syncronous requests to mastermind cocaine application.

        Args:
          handle: API handle name.
          data: request data that will be serialized and sent.
        """
        data = self.service.enqueue(
            handle, msgpack.packb(data),
            attempts=attempts, timeout=timeout).get()
        if isinstance(data, dict):
            if 'Error' in data:
                raise RuntimeError(data['Error'])
            if 'Balancer error' in data:
                raise RuntimeError(data['Balancer error'])
        return data

    @property
    def groups(self):
        return groups.GroupsQuery(self)

    @property
    def namespaces(self):
        return namespaces.NamespacesQuery(self)

    @property
    def couples(self):
        return couples.CouplesQuery(self)

    @property
    def groupsets(self):
        return groupsets.GroupsetsQuery(self)

    @property
    def namespaces_states(self):
        return namespaces_states.NamespacesStatesQuery(self)

    @property
    def remotes(self):
        return self.service.enqueue('get_config_remotes', '')


class DummyClient(object):
    def __getattribute__(self, attr):
        raise RuntimeError('Mastermind client should be bound to query object')
