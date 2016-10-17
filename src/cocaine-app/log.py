import logging
from logging.handlers import SysLogHandler
from logging import FileHandler


def setup_logger(logger_name='logging'):
    """
    TODO: get rid of unused cocaine handler parameters if cocaine handler won't be used anymore
    """

    base_logger = logging.getLogger()
    base_handler = SysLogHandler()
    base_handler.setLevel(logging.INFO)
    base_logger.addHandler(base_handler)
    base_logger.setLevel(logging.INFO)

    root_app_logger = logging.getLogger('mm')
    root_app_logger.propagate = False
    tornado_logger = logging.getLogger('tornado')
    tornado_logger.propagate = False

    worker_logger = logging.getLogger('cocaine.worker')
    worker_logger.propagate = False

    _handler = FileHandler('/var/log/cocaine-core/file.log')
    _handler.setFormatter(logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] [%(name)s] [%(process)d] %(message)s'))

    root_app_logger.addHandler(_handler)
    tornado_logger.addHandler(_handler)
    worker_logger.addHandler(_handler)

    # cocaine.Logger will take care of low-level messages filtering
    root_app_logger.setLevel(logging.DEBUG)
    tornado_logger.setLevel(logging.DEBUG)
    worker_logger.setLevel(logging.INFO)
