"""Python client to the EPICS Archiver Appliance."""
import logging

import pytz
import tzlocal

from aa._version_git import __version__

SCAN = "SCAN"
MONITOR = "MONITOR"

# Make UTC and local timezone easy to get hold of.
LOCALTZ = tzlocal.get_localzone()
UTC = pytz.utc

# Logging utilities
LOG_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
LOG_LEVEL = logging.DEBUG


def set_up_logging(fmt=LOG_FORMAT, level=LOG_LEVEL):
    """Simple logging setup.

    Args:
        fmt: logging format to use
        level: logging level to use

    """
    logging.basicConfig(format=fmt, level=level, datefmt="%Y-%m-%d %I:%M:%S")
