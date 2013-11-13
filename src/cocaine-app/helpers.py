from functools import wraps
import traceback

from cocaine.logging import Logger


logging = Logger()


def handler(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logging.error('Error: ' + str(e) + '\n' + traceback.format_exc())
            return {'Error': str(e)}

    return wrapper
