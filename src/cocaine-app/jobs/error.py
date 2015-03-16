

class JobBrokenError(Exception):
    pass


class RetryError(object):
    def __init__(self, attempts, e):
        self.attempts = attempts
        self.original_e = e

    def __str__(self):
        return 'Error at attempt {0}: {1}'.format(self.attempts, e)
