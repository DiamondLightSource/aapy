"""
Python client to the EPICS Archiver Appliance.
"""
import logging


LOG_FORMAT = '%(levelname)s:  %(message)s'
LOG_LEVEL = logging.INFO
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
