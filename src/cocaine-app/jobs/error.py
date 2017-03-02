

class JobBrokenError(Exception):
    pass


class RetryError(Exception):
    def __init__(self, attempts, e):
        self.attempts = attempts
        self.original_e = e

    def __str__(self):
        return 'error at attempt {0}: {1}'.format(self.attempts, self.original_e)


class JobRequirementError(Exception):
    pass
