import logging

from cocaine.logging import Logger


_cocaine_logger = Logger()


class CocaineHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        # do not work in python 2.6.5 - logging.Handler is an old-style class
        # super(CocaineHandler, self).__init__(level=level)
        logging.Handler.__init__(self, level=level)

    def emit(self, record):
        try:
            levelname = record.levelname.lower()
            # due to bug in cocaine.Logger
            if levelname == 'warning':
                levelname = 'warn'
            log = getattr(_cocaine_logger, levelname)
        except AttributeError:
            _cocaine_logger.warn('No appropriate method for log records '
                'of level "{0}"'.format(record.levelname))
            log = _cocaine_logger.info

        log(self.format(record))


root_logger = logging.getLogger('mm')
root_logger.propagate = False
tornado_logger = logging.getLogger('tornado')
tornado_logger.propagate = False

_handler = CocaineHandler()
_handler.setFormatter(logging.Formatter(fmt='[%(name)s] %(message)s'))

root_logger.addHandler(_handler)
tornado_logger.addHandler(_handler)

# cocaine.Logger will take care of low-level messages filtering
root_logger.setLevel(logging.DEBUG)
tornado_logger.setLevel(logging.DEBUG)
