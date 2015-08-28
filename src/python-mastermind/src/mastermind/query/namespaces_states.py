from mastermind.query import Query


class NamespacesStatesQuery(Query):
    def update(self):
        self.client.request('force_update_namespaces_states', None)
