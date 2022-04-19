import os
import logging
import socket
from logging.handlers import SysLogHandler


class ContextFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = ContextFilter.hostname
        return True


PAPERTRAIL_ADDRESS = os.getenv("PAPERTRAIL_ADDRESS")
PAPERTRAIL_PORT = int(os.getenv("PAPERTRAIL_PORT", 0))


def set_logger(prefix="") -> logging.Logger:
    logger = logging.getLogger()
    if len(logger.handlers) > 0 and not is_papertrail_handler(logger.handlers[0]):
        logger.handlers[0].setLevel(logging.WARNING)
    purge_papertrail_handlers()
    syslog = SysLogHandler(address=(PAPERTRAIL_ADDRESS, PAPERTRAIL_PORT))
    syslog.addFilter(ContextFilter())
    format = f"%(asctime)s %(hostname)s {prefix}: %(message)s"
    formatter = logging.Formatter(format, datefmt="%b %d %H:%M:%S")
    syslog.setFormatter(formatter)
    syslog.setLevel(logging.INFO)
    logger.addHandler(syslog)
    return logger


def purge_papertrail_handlers():
    logger = logging.getLogger()
    for h in logger.handlers:
        if is_papertrail_handler(h):
            logger.removeHandler(h)


def is_papertrail_handler(handler):
    return isinstance(handler, SysLogHandler) and (
        "papertrail" in handler.address or "papertrail" in handler.address[0]
    )
