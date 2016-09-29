from mastermind_core.namespaces.settings.object import SettingsObject


class AuthKeysSettings(SettingsObject):

    PARENT_KEY = 'auth-keys'

    READ = 'read'
    WRITE = 'write'

    VALID_SETTING_KEYS = set([
        READ,
        WRITE,
    ])

    @SettingsObject.settings_property
    def read(self):
        return self._settings.get(self.READ)

    @read.setter
    def read(self, value):
        self._settings[self.READ] = value
        # force setting write auth-key value if not already set
        self._rebuild()

    @SettingsObject.settings_property
    def write(self):
        return self._settings.get(self.WRITE)

    @write.setter
    def write(self, value):
        self._settings[self.WRITE] = value
        # force setting read auth-key value if not already set
        self._rebuild()

    def _rebuild(self):
        if self._settings.get(self.READ) or self._settings.get(self.WRITE):
            self._settings.setdefault(self.READ, '')
            self._settings.setdefault(self.WRITE, '')

    def validate(self):
        super(AuthKeysSettings, self).validate()

        if self.READ in self._settings:
            if not isinstance(self._settings[self.READ], basestring):
                raise ValueError(
                    'Namespace "{}": read auth-key should be a string'.format(
                        self.namespace
                    )
                )

        if self.WRITE in self._settings:
            if not isinstance(self._settings[self.WRITE], basestring):
                raise ValueError(
                    'Namespace "{}": write auth-key should be a string'.format(
                        self.namespace
                    )
                )
