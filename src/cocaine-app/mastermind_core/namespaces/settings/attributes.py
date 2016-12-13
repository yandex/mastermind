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


class SymlinkSettings(SettingsObject):

    PARENT_KEY = 'symlink'

    ENABLE = 'enable'
    SCOPE_LIMIT = 'scope-limit'

    VALID_SETTING_KEYS = set([
        ENABLE,
        SCOPE_LIMIT,
    ])

    SCOPE_LIMIT_NAMESPACE = 'namespace'
    # NOTE: reserved for future implementation (accept any storage namespace)
    # SCOPE_LIMIT_STORAGE = 'storage'
    # NOTE: reserved for future implementation (accept links to external resources)
    # SCOPE_LIMIT_EXTERNAL = 'external'
    VALID_SCOPE_LIMIT = (
        SCOPE_LIMIT_NAMESPACE,
        # SCOPE_LIMIT_STORAGE,
        # SCOPE_LIMIT_EXTERNAL,
    )

    @SettingsObject.settings_property
    def enable(self):
        return self._settings.get(self.ENABLE)

    @enable.setter
    def enable(self, value):
        self._settings[self.ENABLE] = value

    @SettingsObject.settings_property
    def scope_limit(self):
        return self._settings.get(self.SCOPE_LIMIT)

    @scope_limit.setter
    def scope_limit(self, value):
        self._settings[self.SCOPE_LIMIT] = value

    def validate(self):
        super(SymlinkSettings, self).validate()

        if self.ENABLE in self._settings:
            if not isinstance(self._settings[self.ENABLE], bool):
                raise ValueError(
                    'Namespace "{}": attributes symlink enable should be boolean'.format(
                        self.namespace
                    )
                )

        if self._settings and self._settings.get(self.SCOPE_LIMIT) not in self.VALID_SCOPE_LIMIT:
            raise ValueError(
                'Namespace "{ns}": attributes symlink scope-limit expected to be one of '
                '{values}'.format(
                    ns=self.namespace,
                    values=self.VALID_SCOPE_LIMIT,
                )
            )


class AttributesSettings(SettingsObject):

    PARENT_KEY = 'attributes'

    FILENAME = 'filename'
    MIMETYPE = 'mimetype'
    TTL = 'ttl'
    SYMLINK = 'symlink'

    VALID_SETTING_KEYS = set([
        FILENAME,
        MIMETYPE,
        TTL,
        SYMLINK,
    ])

    def _rebuild(self):
        if self.TTL in self._settings:
            self._ttl = TtlSettings(self, self._settings[self.TTL])
        else:
            self._ttl = TtlSettings(self, {})

        if self.SYMLINK in self._settings:
            self._symlink = SymlinkSettings(self, self._settings[self.SYMLINK])
        else:
            self._symlink = SymlinkSettings(self, {})

    @SettingsObject.settings_property
    def filename(self):
        return self._settings.get(self.FILENAME)

    @filename.setter
    def filename(self, value):
        self._settings[self.FILENAME] = value

    @SettingsObject.settings_property
    def mimetype(self):
        return self._settings.get(self.MIMETYPE)

    @mimetype.setter
    def mimetype(self, value):
        self._settings[self.MIMETYPE] = value

    @property
    def ttl(self):
        return self._ttl

    @property
    def symlink(self):
        return self._symlink

    def validate(self):
        super(AttributesSettings, self).validate()

        if self.FILENAME in self._settings:
            if not isinstance(self._settings[self.FILENAME], bool):
                raise ValueError(
                    'Namespace "{}": attributes filename should be boolean'.format(
                        self.namespace
                    )
                )

        if self.MIMETYPE in self._settings:
            if not isinstance(self._settings[self.MIMETYPE], bool):
                raise ValueError(
                    'Namespace "{}": attributes mimetype should be boolean'.format(
                        self.namespace
                    )
                )

        self._ttl.validate()
        self._symlink.validate()
