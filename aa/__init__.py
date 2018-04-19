"""Python client to the EPICS Archiver Appliance."""
import logging


LOG_FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
LOG_LEVEL = logging.DEBUG


def set_up_logging(format=LOG_FORMAT, level=LOG_LEVEL):
    logging.basicConfig(
        format=format, level=level, datefmt='%Y-%m-%d %I:%M:%S'
    )


SCAN = 'SCAN'
MONITOR = 'MONITOR'
