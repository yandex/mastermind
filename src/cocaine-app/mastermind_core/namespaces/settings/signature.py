from mastermind_core.namespaces.settings.object import SettingsObject


class SignatureSettings(SettingsObject):

    PARENT_KEY = 'signature'

    TOKEN = 'token'
    PATH_PREFIX = 'path_prefix'

    VALID_SETTING_KEYS = set([
        TOKEN,
        PATH_PREFIX,
    ])

    @SettingsObject.settings_property
    def token(self):
        return self._settings.get(self.TOKEN)

    @token.setter
    def token(self, value):
        self._settings[self.TOKEN] = value

    @SettingsObject.settings_property
    def path_prefix(self):
        return self._settings.get(self.PATH_PREFIX)

    @path_prefix.setter
    def path_prefix(self, value):
        self._settings[self.PATH_PREFIX] = value

    def validate(self):
        super(SignatureSettings, self).validate()

        if self.TOKEN in self._settings:
            if not isinstance(self._settings[self.TOKEN], basestring):
                raise ValueError(
                    'Namespace "{}": signature token should be a string'.format(
                        self.namespace
                    )
                )

        if self.PATH_PREFIX in self._settings:
            if not isinstance(self._settings[self.PATH_PREFIX], basestring):
                raise ValueError(
                    'Namespace "{}": signature path prefix should be a string'.format(
                        self.namespace
                    )
                )
