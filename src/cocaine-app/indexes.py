import elliptics


class SecondaryIndex(object):
    def __init__(self, idx, key_tpl, meta_session):
        self.idx = idx
        self.key_tpl = key_tpl
        self.meta_session = meta_session

    def __iter__(self):
        for idx in self.meta_session.find_all_indexes([self.idx]):
            yield idx.indexes[0].data

    def __setitem__(self, key, val):
        eid = elliptics.Id(self.key_tpl % key)
        self.meta_session.set_indexes(eid, [self.idx], [val])

    def __getitem__(self, key):
        eid = elliptics.Id(self.key_tpl % key)
        return self.meta_session.list_indexes(eid).get()[0].data

    def __delitem__(self, key):
        eid = elliptics.Id(self.key_tpl % key)
        self.meta_session.set_indexes(eid, [], [])