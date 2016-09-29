import copy
import functools


def _merge_dict(dst, src):
    """ Merges two dicts updating 'dst' keys with those from 'src'
    """
    res = copy.deepcopy(dst)
    for k, val in src.iteritems():
        if k not in dst:
            res[k] = val
        else:
            if not isinstance(val, dict):
                res[k] = val
            else:
                res[k] = _merge_dict(res[k], src[k])
    return res


class SettingsObject(object):

    PARENT_KEY = None

    VALID_SETTING_KEYS = set()

    def __init__(self, parent, settings, on_save_callback=None):
        self._parent = parent
        self._settings = settings
        self._dirty = False
        self._on_save_callback = on_save_callback

        if self._parent:
            # TODO: remove this hack
            self.namespace = self._parent.namespace

        self._rebuild()

    def _rebuild(self):
        pass

    def make_dirty(self, dirty=True):
        current = self
        while current._parent is not None:
            current = current._parent
        current._dirty = True

    class settings_property(property):
        def __set__(self, obj, value):
            make_dirty = self.__get__(obj) != value

            super(SettingsObject.settings_property, self).__set__(obj, value)

            if make_dirty:
                obj._insert_key()
                obj.make_dirty()

    def _insert_key(self):
        """ Insert self into a parents' tree if not already a subtree """
        current = self
        while current._parent is not None:
            if not current.PARENT_KEY:
                return
            if current.PARENT_KEY in current._parent._settings:
                return
            current._parent._settings[current.PARENT_KEY] = current._settings
            current = current._parent

    def dump(self):
        return self._settings

    def validate(self):
        for setting_key in self._settings.iterkeys():
            if setting_key not in self.VALID_SETTING_KEYS:
                raise ValueError('Unsupported setting "{}"'.format(setting_key))

    def save(self):
        if self._on_save_callback:
            self._on_save_callback(self, dirty=self._dirty)
        self._dirty = False

    def update(self, settings):
        """ Merge @settings with current settings object """
        self._settings = _merge_dict(self._settings, settings)
        self._rebuild()
        self.make_dirty(dirty=True)
        self._insert_key()

    def set(self, settings):
        """ Replace current object settings object with @settings """
        self._settings = settings
        self._rebuild()
        self.make_dirty(dirty=True)
        self._insert_key()

    def __str__(self):
        return str(self.dump())
