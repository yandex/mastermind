class MastermindError(Exception):
    @property
    def code(self):
        return MASTERMIND_ERROR_CODES[type(self)]

    @staticmethod
    def make_error(code, msg):
        if code not in MASTERMIND_ERROR_CLS:
            raise ValueError('Unknown error code {}'.format(code))
        return MASTERMIND_ERROR_CLS[code](msg)

GENERAL_ERROR_CODE = 1024

MASTERMIND_ERROR_CODES = {
    MastermindError: GENERAL_ERROR_CODE
}

MASTERMIND_ERROR_CLS = dict((v, k) for k, v in MASTERMIND_ERROR_CODES.iteritems())
