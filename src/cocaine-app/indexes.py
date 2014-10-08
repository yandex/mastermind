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
        eid = self.meta_session.transform(self.key_tpl % key)
        self.meta_session.set_indexes(eid, [self.idx], [val])

    def __getitem__(self, key):
        eid = self.meta_session.transform(self.key_tpl % key)
        return self.meta_session.list_indexes(eid).get()[0].data

    def __delitem__(self, key):
        eid = self.meta_session.transform(self.key_tpl % key)
        self.meta_session.set_indexes(eid, [], [])


class TagSecondaryIndex(object):
    def __init__(self, main_idx, idx_tpl, key_tpl, meta_session, logger=None, namespace=None):
        self.main_idx = main_idx
        self.idx_tpl = idx_tpl
        self.key_tpl = key_tpl
        self.meta_session = meta_session.clone()
        if namespace:
            self.meta_session.set_namespace(namespace)
        self.logger = logger

    def __iter__(self):
        idxes = [idx.id for idx in
            self.meta_session.find_all_indexes([self.main_idx]).get()]

        for data in self._iter_keys(idxes):
            yield data

    def _iter_keys(self, keys):
        if not keys:
            return
        count = 0
        for resp in self.meta_session.bulk_read(keys).get():
            count += 1
            yield resp.data

        if self.logger and count != len(keys):
            self.logger.error('Inconsistent index {0}: index keys {1} keys, '
                'read keys {2}'.format(self.main_idx))

    def tagged(self, tag):
        idxes = [idx.id for idx in
            self.meta_session.find_all_indexes([self.main_idx, self.idx_tpl % tag])]

        for data in self._iter_keys(idxes):
            yield data

    def __setitem__(self, key, val):
        eid = self.meta_session.transform(self.key_tpl % key)
        self.meta_session.write_data(eid, val)

    def set_tag(self, key, tag):
        eid = self.meta_session.transform(self.key_tpl % key)
        self.meta_session.set_indexes(eid, [self.main_idx, self.idx_tpl % tag], ['', ''])
