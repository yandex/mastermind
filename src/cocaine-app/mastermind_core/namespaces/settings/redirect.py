from mastermind_core.namespaces.settings.object import SettingsObject


class RedirectSettings(SettingsObject):

    PARENT_KEY = 'redirect'

    EXPIRE_TIME = 'expire-time'
    ADD_ORIG_PATH_QUERY_ARG = 'add-orig-path-query-arg'
    CONTENT_LENGTH_THRESHOLD = 'content-length-threshold'
    QUERY_ARGS = 'query-args'

    VALID_SETTING_KEYS = set([
        EXPIRE_TIME,
        ADD_ORIG_PATH_QUERY_ARG,
        CONTENT_LENGTH_THRESHOLD,
        QUERY_ARGS,
    ])

    @SettingsObject.settings_property
    def expire_time(self):
        return self._settings.get(self.EXPIRE_TIME)

    @expire_time.setter
    def expire_time(self, value):
        self._settings[self.EXPIRE_TIME] = value

    @SettingsObject.settings_property
    def add_orig_path_query_arg(self):
        return self._settings.get(self.ADD_ORIG_PATH_QUERY_ARG)

    @add_orig_path_query_arg.setter
    def add_orig_path_query_arg(self, value):
        self._settings[self.ADD_ORIG_PATH_QUERY_ARG] = value

    @SettingsObject.settings_property
    def content_length_threshold(self):
        return self._settings.get(self.CONTENT_LENGTH_THRESHOLD)

    @content_length_threshold.setter
    def content_length_threshold(self, value):
        self._settings[self.CONTENT_LENGTH_THRESHOLD] = value

    @SettingsObject.settings_property
    def query_args(self):
        return self._settings.get(self.QUERY_ARGS)

    @query_args.setter
    def query_args(self, value):
        self._settings[self.QUERY_ARGS] = value

    def validate(self):
        super(RedirectSettings, self).validate()

        if self.EXPIRE_TIME in self._settings:
            if not isinstance(self._settings[self.EXPIRE_TIME], int):
                raise ValueError(
                    'Namespace "{}": redirect expire-time should be a positive integer'.format(
                        self.namespace
                    )
                )
            if self._settings[self.EXPIRE_TIME] <= 0:
                raise ValueError(
                    'Namespace "{}": redirect expire-time should be a positive integer'.format(
                        self.namespace
                    )
                )

        if self.ADD_ORIG_PATH_QUERY_ARG in self._settings:
            if not isinstance(self._settings[self.ADD_ORIG_PATH_QUERY_ARG], bool):
                raise ValueError(
                    'Namespace "{}": redirect add orig path query arg should be boolean'.format(
                        self.namespace
                    )
                )

        if self.CONTENT_LENGTH_THRESHOLD in self._settings:
            if not isinstance(self._settings[self.CONTENT_LENGTH_THRESHOLD], int):
                raise ValueError(
                    'Namespace "{}": redirect content length threshold should be a '
                    'non-negative integer or -1'.format(
                        self.namespace
                    )
                )
            if self._settings[self.CONTENT_LENGTH_THRESHOLD] < -1:
                raise ValueError(
                    'Namespace "{}": redirect content length threshold should be a '
                    'non-negative integer or -1'.format(
                        self.namespace
                    )
                )

        if self.QUERY_ARGS in self._settings:
            if not isinstance(self._settings[self.QUERY_ARGS], (list, tuple)):
                raise ValueError(
                    'Namespace "{}": redirect query args should be a list of strings'.format(
                        self.namespace
                    )
                )
            for query_arg in self._settings[self.QUERY_ARGS]:
                if not isinstance(query_arg, basestring):
                    raise ValueError(
                        'Namespace "{}": redirect query args should be a list of strings'.format(
                            self.namespace
                        )
                    )
