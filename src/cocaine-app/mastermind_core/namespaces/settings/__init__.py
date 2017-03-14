from mastermind_core.namespaces.settings.object import SettingsObject
from mastermind_core.namespaces.settings.redirect import RedirectSettings
from mastermind_core.namespaces.settings.signature import SignatureSettings
from mastermind_core.namespaces.settings.auth_keys import AuthKeysSettings
from mastermind_core.namespaces.settings.features import FeaturesSettings
from mastermind_core.namespaces.settings.attributes import AttributesSettings
from mastermind_core.namespaces.settings.owner import OwnerSettings


class NamespaceSettings(SettingsObject):

    # __service is a special hidden key with internal namespace settings
    __SERVICE = '__service'
    __DELETED = 'is_deleted'

    NAMESPACE = 'namespace'
    SUCCESS_COPIES_NUM = 'success-copies-num'
    GROUPS_COUNT = 'groups-count'
    STATIC_COUPLE = 'static-couple'
    MIN_UNITS = 'min-units'
    ADD_UNITS = 'add-units'
    REDIRECT = 'redirect'
    CHECK_FOR_UPDATE = 'check-for-update'
    RESERVED_SPACE_PERCENTAGE = 'reserved-space-percentage'
    SIGNATURE = 'signature'
    AUTH_KEYS = 'auth-keys'
    FEATURES = 'features'
    ATTRIBUTES = 'attributes'
    OWNER = 'owner'

    VALID_SETTING_KEYS = set([
        __SERVICE,
        NAMESPACE,
        SUCCESS_COPIES_NUM,
        GROUPS_COUNT,
        STATIC_COUPLE,
        MIN_UNITS,
        ADD_UNITS,
        REDIRECT,
        CHECK_FOR_UPDATE,
        RESERVED_SPACE_PERCENTAGE,
        SIGNATURE,
        AUTH_KEYS,
        FEATURES,
        ATTRIBUTES,
        OWNER,
    ])

    SUCCESS_COPIES_ANY = 'any'
    SUCCESS_COPIES_QUORUM = 'quorum'
    SUCCESS_COPIES_ALL = 'all'
    VALID_SUCCESS_COPIES = (SUCCESS_COPIES_ANY, SUCCESS_COPIES_QUORUM, SUCCESS_COPIES_ALL)

    def __init__(self, settings, on_save_callback=None):
        if 'namespace' not in settings:
            raise ValueError('Namespace is not defined for namespace settings')
        self.namespace = settings['namespace']

        settings.setdefault(self.__SERVICE, {})

        # define all supported subsections
        self._redirect = None
        self._signature = None
        self._auth_keys = None
        self._features = None
        self._attributes = None
        self._owner = None

        super(NamespaceSettings, self).__init__(
            parent=None,
            settings=settings,
            on_save_callback=on_save_callback,
        )

    def _rebuild(self):
        self._redirect = RedirectSettings(self, self._settings.get(self.REDIRECT, {}))
        self._signature = SignatureSettings(self, self._settings.get(self.SIGNATURE, {}))
        self._auth_keys = AuthKeysSettings(self, self._settings.get(self.AUTH_KEYS, {}))
        self._features = FeaturesSettings(self, self._settings.get(self.FEATURES, {}))
        self._attributes = AttributesSettings(self, self._settings.get(self.ATTRIBUTES, {}))
        self._owner = OwnerSettings(self, self._settings.get(self.OWNER, {}))

    @SettingsObject.settings_property
    def deleted(self):
        return self._settings.get(self.__SERVICE, {}).get(self.__DELETED, False)

    def delete(self):
        _service = self._settings.setdefault(self.__SERVICE, {})
        _service[self.__DELETED] = True
        self.make_dirty()

    @property
    def redirect(self):
        return self._redirect

    @property
    def signature(self):
        return self._signature

    @property
    def auth_keys(self):
        return self._auth_keys

    @property
    def features(self):
        return self._features

    @property
    def attributes(self):
        return self._attributes

    @property
    def owner(self):
        return self._owner

    @SettingsObject.settings_property
    def min_units(self):
        return self._settings.get(self.MIN_UNITS)

    @min_units.setter
    def min_units(self, value):
        self._settings[self.MIN_UNITS] = value

    @SettingsObject.settings_property
    def add_units(self):
        return self._settings.get(self.ADD_UNITS)

    @add_units.setter
    def add_units(self, value):
        self._settings[self.ADD_UNITS] = value

    @SettingsObject.settings_property
    def groups_count(self):
        return self._settings.get(self.GROUPS_COUNT)

    @groups_count.setter
    def groups_count(self, value):
        self._settings[self.GROUPS_COUNT] = value

    @SettingsObject.settings_property
    def static_couple(self):
        return self._settings.get(self.STATIC_COUPLE)

    @static_couple.setter
    def static_couple(self, value):
        self._settings[self.STATIC_COUPLE] = value

    @SettingsObject.settings_property
    def success_copies_num(self):
        return self._settings.get(self.SUCCESS_COPIES_NUM)

    @success_copies_num.setter
    def success_copies_num(self, value):
        self._settings[self.SUCCESS_COPIES_NUM] = value

    @SettingsObject.settings_property
    def reserved_space_percentage(self):
        return self._settings.get(self.RESERVED_SPACE_PERCENTAGE)

    @reserved_space_percentage.setter
    def reserved_space_percentage(self, value):
        self._settings[self.RESERVED_SPACE_PERCENTAGE] = value

    @SettingsObject.settings_property
    def check_for_update(self):
        return self._settings.get(self.CHECK_FOR_UPDATE)

    @check_for_update.setter
    def check_for_update(self, value):
        self._settings[self.CHECK_FOR_UPDATE] = value

    def _validate_first_level_settings(self):
        if self.GROUPS_COUNT not in self._settings:
            if self.STATIC_COUPLE not in self._settings:
                raise ValueError('Namespace "{}": settings require groups count'.format(self.namespace))
            else:
                # forcing 'groups-count' to match the length of the static couple
                # TODO: maybe this should be forced on the client side?
                self._settings[self.GROUPS_COUNT] = len(self._settings[self.STATIC_COUPLE])
        else:
            if not isinstance(self._settings[self.GROUPS_COUNT], int):
                raise ValueError('Namespace "{}": settings require groups count'.format(self.namespace))
            if self._settings[self.GROUPS_COUNT] <= 0:
                raise ValueError('Namespace "{}": groups count should be a positive integer'.format(self.namespace))

            if self.STATIC_COUPLE in self._settings:
                if len(self._settings[self.STATIC_COUPLE]) != self.groups_count:
                    raise ValueError(
                        'Namespace "{ns}": static couple length {static_couple_length} '
                        'does not match groups count {groups_count}'.format(
                            ns=self.namespace,
                            static_couple_length=len(self._settings[self.STATIC_COUPLE]),
                            groups_count=self.groups_count,
                        )
                    )

        if self._settings.get(self.SUCCESS_COPIES_NUM) not in self.VALID_SUCCESS_COPIES:
            raise ValueError(
                'Namespace "{ns}": success copies num expected to be one of {values}'.format(
                    ns=self.namespace,
                    values=self.VALID_SUCCESS_COPIES,
                )
            )

        if self.MIN_UNITS in self._settings:
            if not isinstance(self._settings[self.MIN_UNITS], int):
                raise ValueError(
                    'Namespace "{}": min units should be a non-negative integer'.format(
                        self.namespace
                    )
                )
            if self._settings[self.MIN_UNITS] < 0:
                raise ValueError(
                    'Namespace "{}": min units should be a non-negative integer'.format(
                        self.namespace
                    )
                )

        if self.ADD_UNITS in self._settings:
            if not isinstance(self._settings[self.ADD_UNITS], int):
                raise ValueError(
                    'Namespace "{}": add units should be a non-negative integer'.format(
                        self.namespace
                    )
                )
            if self._settings[self.ADD_UNITS] < 0:
                raise ValueError(
                    'Namespace "{}": add units should be a non-negative integer'.format(
                        self.namespace
                    )
                )

        if self.CHECK_FOR_UPDATE in self._settings:
            if not isinstance(self._settings[self.CHECK_FOR_UPDATE], bool):
                raise ValueError(
                    'Namespace "{}": check-for-update should be a boolean value'.format(
                        self.namespace
                    )
                )

        if self.RESERVED_SPACE_PERCENTAGE in self._settings:
            if not isinstance(self._settings[self.RESERVED_SPACE_PERCENTAGE], (int, float)):
                raise ValueError(
                    'Namespace "{}": reserved space percentage should be a float value '
                    'in [0.0; 1.0] interval'.format(
                        self.namespace
                    )
                )

            if not 0 <= self._settings[self.RESERVED_SPACE_PERCENTAGE] <= 1:
                raise ValueError(
                    'Namespace "{}": reserved space percentage should be a float value '
                    'in [0.0; 1.0] interval'.format(
                        self.namespace
                    )
                )

    def validate(self):
        super(NamespaceSettings, self).validate()
        self._validate_first_level_settings()
        self._redirect.validate()
        self._signature.validate()
        self._auth_keys.validate()
        self._features.validate()
        self._attributes.validate()
        self._owner.validate()

        # Checking redirect and signature linked values
        linked_keys = (
            self.redirect.expire_time,
            self.signature.token,
            self.signature.path_prefix,
        )

        if not all(linked_keys) and any(linked_keys):
            raise ValueError(
                'Namespace "{}": signature token, signature path prefix and redirect '
                'expire time should be set simultaneously'.format(
                    self.namespace,
                )
            )

        # check-for-update cannot be disabled if ttl is enabled
        is_ttl_enabled = self.attributes.ttl.enable is True
        is_check_for_update_disabled = self.check_for_update is False
        if is_ttl_enabled and is_check_for_update_disabled:
            raise ValueError(
                'Namespace "{}": ttl attribute cannot be enabled when check-for-update '
                'is disabled'.format(
                    self.namespace,
                )
            )
