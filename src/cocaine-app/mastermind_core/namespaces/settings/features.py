from mastermind_core.namespaces.settings.object import SettingsObject


class FeaturesMultipartSettings(SettingsObject):

    PARENT_KEY = 'multipart'

    CONTENT_LENGTH_THRESHOLD = 'content-length-threshold'

    VALID_SETTING_KEYS = set([
        CONTENT_LENGTH_THRESHOLD,
    ])

    @SettingsObject.settings_property
    def content_length_threshold(self):
        return self._settings.get(self.CONTENT_LENGTH_THRESHOLD)

    @content_length_threshold.setter
    def content_length_threshold(self, value):
        self._settings[self.CONTENT_LENGTH_THRESHOLD] = value

    def validate(self):
        super(FeaturesMultipartSettings, self).validate()

        if self.CONTENT_LENGTH_THRESHOLD in self._settings:
            if not isinstance(self._settings[self.CONTENT_LENGTH_THRESHOLD], int):
                raise ValueError(
                    'Namespace "{}": multipart content length threshold should be '
                    'non-negative integer'.format(
                        self.namespace
                    )
                )

            if self._settings[self.CONTENT_LENGTH_THRESHOLD] < -1:
                raise ValueError(
                    'Namespace "{}": multipart content length threshold should be '
                    'non-negative integer or -1'.format(
                        self.namespace
                    )
                )


class FeaturesSettings(SettingsObject):

    PARENT_KEY = 'features'

    CUSTOM_EXPIRE_TIME = 'custom-expiration-time'
    SELECT_COUPLE_TO_UPLOAD = 'select-couple-to-upload'
    MULTIPART = 'multipart'

    VALID_SETTING_KEYS = set([
        CUSTOM_EXPIRE_TIME,
        SELECT_COUPLE_TO_UPLOAD,
        MULTIPART,
    ])

    def _rebuild(self):
        if self.MULTIPART in self._settings:
            self._multipart = FeaturesMultipartSettings(self, self._settings[self.MULTIPART])
        else:
            self._multipart = FeaturesMultipartSettings(self, {})

    @SettingsObject.settings_property
    def custom_expire_time(self):
        return self._settings.get(self.CUSTOM_EXPIRE_TIME)

    @custom_expire_time.setter
    def custom_expire_time(self, value):
        self._settings[self.CUSTOM_EXPIRE_TIME] = value

    @SettingsObject.settings_property
    def select_couple_to_upload(self):
        return self._settings.get(self.SELECT_COUPLE_TO_UPLOAD)

    @select_couple_to_upload.setter
    def select_couple_to_upload(self, value):
        self._settings[self.SELECT_COUPLE_TO_UPLOAD] = value

    @property
    def multipart(self):
        return self._multipart

    def validate(self):
        super(FeaturesSettings, self).validate()

        if self.CUSTOM_EXPIRE_TIME in self._settings:
            if not isinstance(self._settings[self.CUSTOM_EXPIRE_TIME], bool):
                raise ValueError(
                    'Namespace "{}": multipart custom expire time should be boolean'.format(
                        self.namespace
                    )
                )

        if self.SELECT_COUPLE_TO_UPLOAD in self._settings:
            if not isinstance(self._settings[self.SELECT_COUPLE_TO_UPLOAD], bool):
                raise ValueError(
                    'Namespace "{}": multipart select couple to upload should be boolean'.format(
                        self.namespace
                    )
                )

        self._multipart.validate()
