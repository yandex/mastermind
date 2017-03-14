from mastermind_core.namespaces.settings.object import SettingsObject


class OwnerSettings(SettingsObject):

    PARENT_KEY = 'owner'

    ID = 'id'

    VALID_SETTING_KEYS = set([
        ID,
    ])

    @SettingsObject.settings_property
    def id(self):
        return self._settings.get(self.ID)

    @id.setter
    def id(self, value):
        self._settings[self.ID] = value

    def validate(self):
        super(OwnerSettings, self).validate()

        if self.ID in self._settings:
            if not isinstance(self._settings[self.ID], int):
                raise ValueError(
                    'Namespace "{}": owner\'s id should be an integer'.format(
                        self.namespace,
                    )
                )
