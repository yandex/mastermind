import logging
import sys
import threading

from cocaine.logging.log_message import Message
from cocaine.logging.logger import VERBOSITY_LEVELS
from cocaine.services import Service


class Logger(Service):

    __instances_lock = threading.Lock()
    _instances = {}

    def __new__(cls, logger_name='logging'):
        with cls.__instances_lock:
            if logger_name not in cls._instances:
                instance = object.__new__(cls)

                cls._instances[logger_name] = instance
                instance._counter = 0
                instance._lock = threading.Lock()
                try:
                    instance.target = "app/%s" % sys.argv[sys.argv.index("--app") + 1]
                except ValueError:
                    instance.target = "app/standalone"

                def wrapper(level):
                    target = instance.target

                    def on_emit(message):
                        with instance._lock:
                            instance._counter += 1
                            instance._writableStream.write(Message("Message",
                                instance._counter, level, target, str(message)).pack())

                    return on_emit

                for level_name, level in VERBOSITY_LEVELS.items():
                    setattr(instance, level_name, wrapper(level))

            return cls._instances[logger_name]

    def _on_message(self, args):
        # This is ESSENTIAL for logger to work properly
        pass


class CocaineHandler(logging.Handler):
    def __init__(self, logger=None, *args, **kwargs):
        logging.Handler.__init__(self)

        self._logger = logger or Logger()
        self.level_binds = {
            logging.DEBUG: self._logger.debug,
            logging.INFO: self._logger.info,
            logging.WARNING: self._logger.warn,
            logging.ERROR: self._logger.error
        }

    def emit(self, record):
        def dummy(*args):  # pragma: no cover
            pass
        msg = self.format(record)
        self.level_binds.get(record.levelno, dummy)(msg)


def setup_logger(logger_name='logging'):
    cocaine_logger = Logger(logger_name)

    root_logger = logging.getLogger('mm')
    root_logger.propagate = False
    tornado_logger = logging.getLogger('tornado')
    tornado_logger.propagate = False

    _handler = CocaineHandler(cocaine_logger)
    _handler.setFormatter(logging.Formatter(fmt='[%(name)s] [%(process)d] %(message)s'))

    root_logger.addHandler(_handler)
    tornado_logger.addHandler(_handler)

    # cocaine.Logger will take care of low-level messages filtering
    root_logger.setLevel(logging.DEBUG)
    tornado_logger.setLevel(logging.DEBUG)
