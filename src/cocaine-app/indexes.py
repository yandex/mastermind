from Queue import Queue

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

    BATCH_SIZE = 500

    def __init__(self, main_idx, idx_tpl, key_tpl, meta_session, logger=None, namespace=None, batch_size=BATCH_SIZE):
        self.main_idx = main_idx
        self.idx_tpl = idx_tpl
        self.key_tpl = key_tpl
        self.meta_session = meta_session.clone()
        if namespace:
            self.meta_session.set_namespace(namespace)
        self.batch_size = batch_size
        self.logger = logger

    def __iter__(self):
        idxes = [idx.id for idx in
            self.meta_session.find_all_indexes([self.main_idx]).get()]

        for data in self._iter_keys(idxes):
            yield data

    def tagged(self, tag):
        idxes = [idx.id for idx in
            self.meta_session.find_all_indexes([self.main_idx, self.idx_tpl % tag])]

        for data in self._iter_keys(idxes):
            yield data

    def __setitem__(self, key, val):
        eid = self.meta_session.transform(self.key_tpl % key)
        self.meta_session.write_data(eid, val)

    def __getitem__(self, key):
        eid = self.meta_session.transform(self.key_tpl % key)
        return self.meta_session.read_latest(eid).get()[0].data

    def set_tag(self, key, tag=None):
        eid = self.meta_session.transform(self.key_tpl % key)
        tags = [self.main_idx]
        if tag:
            tags.append(self.idx_tpl % tag)
        self.meta_session.set_indexes(eid, tags, [''] * len(tags))

    def _fetch_response_data(self, req):
        data = None
        try:
            result = req[1]
            result.wait()
            data = result.get()[0].data
        except Exception as e:
            self.logger.error('Failed to fetch record from tagged index: {0}'.format(req[0]))
        return data

    def _iter_keys(self, keys):
        if not keys:
            return

        q = Queue(self.batch_size)
        count = 0

        for k in keys:
            if not q.full():
                q.put((k, self.meta_session.read_latest(k)))
            else:
                data = self._fetch_response_data(q.get())
                if data:
                    yield data

        while q.qsize():
            data = self._fetch_response_data(q.get())
            if data:
                yield data
