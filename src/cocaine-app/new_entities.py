from infrastructure_cache import cache

class Host(object):
    def __init__(self, addr):
        self.addr = addr
        self.dc = None
        self.hostname = None
        self.nodes = []

    def update(self, state):
        if 'dc' in state:
            self.dc = state['dc']
        if 'name' in state:
            self.hostname = state['name']

    @property
    def hostname_or_not(self):
        return self.hostname

    @property
    def dc_or_not(self):
        return self.dc

    @property
    def parents(self):
        return cache.get_host_tree(self.hostname)

    @property
    def full_path(self):
        parent = self.parents
        parts = [parent['name']]
        while 'parent' in parent:
            parent = parent['parent']
            parts.append(parent['name'])
        return '|'.join(reversed(parts))

    def index(self):
        return self.__str__()

    def __eq__(self, other):

        if isinstance(other, basestring):
            return self.addr == other

        if isinstance(other, Host):
            return self.addr == other.addr

        return False

    def __hash__(self):
        return hash(self.__str__())

    def __repr__(self):
        return ('<Host object: addr=%s, nodes=[%s] >' %
                (self.addr, ', '.join((repr(n) for n in self.nodes))))

    def __str__(self):
        return self.addr
