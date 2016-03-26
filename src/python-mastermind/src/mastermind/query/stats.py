class Stats(object):
    def __init__(self, raw_data):
        self.effective_space = raw_data['effective_space']
        self.free_effective_space = raw_data['free_effective_space']
        self.free_reserved_space = raw_data['free_reserved_space']

    def __repr__(self):
        # TODO: convert numbers to human-readable form
        return (
            '<Stats: '
            'effective space: {effective_space} bytes, '
            'free effective space: {free_effective_space} bytes, '
            'free reserved space: {free_reserved_space} bytes'
            '>'
        ).format(
            effective_space=self.effective_space,
            free_effective_space=self.free_effective_space,
            free_reserved_space=self.free_reserved_space,
        )
