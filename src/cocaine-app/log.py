import logging
from logging.handlers import SysLogHandler

from cocaine.logger import CocaineHandler


def setup_logger(logger_name='logging'):

    base_logger = logging.getLogger()
    base_handler = SysLogHandler()
    base_handler.setLevel(logging.INFO)
    base_logger.addHandler(base_handler)
    base_logger.setLevel(logging.INFO)

    root_app_logger = logging.getLogger('mm')
    root_app_logger.propagate = False
    tornado_logger = logging.getLogger('tornado')
    tornado_logger.propagate = False

    _handler = CocaineHandler()
    _handler._logger._name = logger_name
    _handler.setFormatter(logging.Formatter(fmt='[%(name)s] [%(process)d] %(message)s'))

    root_app_logger.addHandler(_handler)
    tornado_logger.addHandler(_handler)

    # cocaine.Logger will take care of low-level messages filtering
    root_app_logger.setLevel(logging.DEBUG)
    tornado_logger.setLevel(logging.DEBUG)
