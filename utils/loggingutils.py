from __future__ import annotations

import logging
import os
import socket
from contextlib import contextmanager
from logging.handlers import SysLogHandler


class ContextFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = ContextFilter.hostname
        return True


PAPERTRAIL_ADDRESS = os.getenv("PAPERTRAIL_ADDRESS", "")
PAPERTRAIL_PORT = int(os.getenv("PAPERTRAIL_PORT", "0"))


@contextmanager
def papertrail_logger(prefix=""):
    handler_format = f"%(asctime)s %(hostname)s {prefix}: %(message)s"
    formatter = logging.Formatter(handler_format, datefmt="%b %d %H:%M:%S")

    logger = logging.getLogger()
    current_handlers = get_current_papertrail_handlers()
    current_formatters = []
    if not current_handlers:
        syslog = SysLogHandler(address=(PAPERTRAIL_ADDRESS, PAPERTRAIL_PORT))
        syslog.addFilter(ContextFilter())
        syslog.setFormatter(formatter)
        syslog.setLevel(logging.INFO)
        logger.addHandler(syslog)
    else:
        for handler in current_handlers:
            current_formatters.append(handler.formatter)
            handler.setFormatter(formatter)
    yield logger

    if not current_handlers:
        logger.removeHandler(syslog)
    else:
        for i, handler in enumerate(current_handlers):
            handler.setFormatter(current_formatters[i])


def set_logger(prefix="") -> logging.Logger:
    logger = logging.getLogger()
    purge_papertrail_handlers()
    syslog = SysLogHandler(address=(PAPERTRAIL_ADDRESS, PAPERTRAIL_PORT))
    syslog.addFilter(ContextFilter())
    handler_format = f"%(asctime)s %(hostname)s {prefix}: %(message)s"
    formatter = logging.Formatter(handler_format, datefmt="%b %d %H:%M:%S")
    syslog.setFormatter(formatter)
    syslog.setLevel(logging.INFO)
    logger.addHandler(syslog)
    return logger


def purge_papertrail_handlers():
    logger = logging.getLogger()
    current_papertrail_handlers = get_current_papertrail_handlers()
    for handler in current_papertrail_handlers:
        logger.removeHandler(handler)


def get_current_papertrail_handlers():
    res: list[logging.Handler] = []
    logger = logging.getLogger()
    for handler in logger.handlers:
        if is_papertrail_handler(handler):
            res.append(handler)
    return res


def is_papertrail_handler(handler):
    return isinstance(handler, SysLogHandler) and (
        "papertrail" in handler.address or "papertrail" in handler.address[0]
    )
