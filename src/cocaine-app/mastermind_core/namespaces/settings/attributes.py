import re

from mastermind_core.namespaces.settings.object import SettingsObject


class TtlSettings(SettingsObject):

    PARENT_KEY = 'ttl'

    ENABLE = 'enable'
    MINIMUM = 'minimum'
    MAXIMUM = 'maximum'

    VALID_SETTING_KEYS = set([
        ENABLE,
        MINIMUM,
        MAXIMUM,
    ])

    @SettingsObject.settings_property
    def enable(self):
        return self._settings.get(self.ENABLE)

    @enable.setter
    def enable(self, value):
        self._settings[self.ENABLE] = value

    @SettingsObject.settings_property
    def minimum(self):
        return self._settings.get(self.MINIMUM)

    @minimum.setter
    def minimum(self, value):
        self._settings[self.MINIMUM] = value

    @SettingsObject.settings_property
    def maximum(self):
        return self._settings.get(self.MAXIMUM)

    @maximum.setter
    def maximum(self, value):
        self._settings[self.MAXIMUM] = value

    TIME_UNITS_RE = re.compile('^(\d+)(?:[smhd])$')

    @staticmethod
    def __valid_time_units(time_units):
        match = TtlSettings.TIME_UNITS_RE.match(time_units)
        if match is None:
            return False
        time_units_num_val = int(match.group(1))
        if time_units_num_val <= 0:
            return False
        return True

    def validate(self):
        super(TtlSettings, self).validate()

        if self.ENABLE in self._settings:
            if not isinstance(self._settings[self.ENABLE], bool):
                raise ValueError(
                    'Namespace "{}": attributes ttl enable should be boolean'.format(
                        self.namespace
                    )
                )

        if self.MINIMUM in self._settings:
            if not self.__valid_time_units(self._settings[self.MINIMUM]):
                raise ValueError(
                    'Namespace "{}": attributes ttl minimum should be a valid time unit '
                    '(a positive integer followed by "s" (seconds), "m" (minutes), "h" '
                    '(hours) or "d" (days))'.format(
                        self.namespace
                    )
                )

        if self.MAXIMUM in self._settings:
            if not self.__valid_time_units(self._settings[self.MAXIMUM]):
                raise ValueError(
                    'Namespace "{}": attributes ttl maximum should be a valid time unit '
                    '(a positive integer followed by "s" (seconds), "m" (minutes), "h" '
                    '(hours) or "d" (days))'.format(
                        self.namespace
                    )
                )


class AttributesSettings(SettingsObject):

    PARENT_KEY = 'attributes'

    FILENAME = 'filename'
    TTL = 'ttl'

    VALID_SETTING_KEYS = set([
        FILENAME,
        TTL,
    ])

    def _rebuild(self):
        if self.TTL in self._settings:
            self._ttl = TtlSettings(self, self._settings[self.TTL])
        else:
            self._ttl = TtlSettings(self, {})

    @SettingsObject.settings_property
    def filename(self):
        return self._settings.get(self.FILENAME)

    @filename.setter
    def filename(self, value):
        self._settings[self.FILENAME] = value

    @property
    def ttl(self):
        return self._ttl

    def validate(self):
        super(AttributesSettings, self).validate()

        if self.FILENAME in self._settings:
            if not isinstance(self._settings[self.FILENAME], bool):
                raise ValueError(
                    'Namespace "{}": attributes filename should be boolean'.format(
                        self.namespace
                    )
                )

        self._ttl.validate()
