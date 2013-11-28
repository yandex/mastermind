class EllAsyncResult(object):

    def __init__(self, result, entry_type):
        self.result = result
        self.entry_type = entry_type

    def get(self):
        if getattr(self.result, 'get', None):
            return self.result.get()

        return [self.entry_type(self.result)]


class EllReadResult(object):
    def __init__(self, entry):
        self.data = entry


class EllLookupResult(object):
    def __init__(self, entry):
        self.entry = entry
