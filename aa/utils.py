"""Miscellaneous utility functions."""
from __future__ import print_function

import logging
from datetime import datetime
from typing import Dict, List, Tuple

import pytz
import tzlocal

__all__ = [
    "utc_datetime",
    "utc_now",
    "EPOCH",
    "datetime_to_epoch",
    "epoch_to_datetime",
    "add_local_timezone",
    "year_timestamp",
    "print_raw_bytes",
    "binary_search",
    "dict_to_tuples",
]


def utc_datetime(*args):
    # pylint: disable=no-value-for-parameter
    return pytz.utc.localize(datetime(*args))


def utc_now():
    return pytz.utc.localize(datetime.now())


EPOCH = utc_datetime(1970, 1, 1)


def datetime_to_epoch(dt):
    return int((dt - EPOCH).total_seconds())


def epoch_to_datetime(secs):
    return datetime.fromtimestamp(secs, tz=pytz.utc)


def add_local_timezone(dt):
    """Add the local timezone to a naive datetime.

    The tzlocal module is used to deduce the local timezone.

    Args:
        dt: naive datetime

    Raises:
        AssertionError: if dt already has a timezone
    """
    assert dt.tzinfo is None
    localtz = tzlocal.get_localzone()
    logging.warning("Assuming timezone for {} is {}".format(dt, localtz))
    return localtz.localize(dt)


def year_timestamp(year):
    return (datetime(year, 1, 1) - datetime(1970, 1, 1)).total_seconds()


def print_raw_bytes(byte_seq):
    for b in byte_seq:
        print("\\x{:02x}".format(ord(b)), end="")
    print("")


def binary_search(seq, f, target):
    """Find no such that f(seq[no-1]) <= target and f(seq[no]) > target.

    If target < f(seq[0]), return 0
    If target > f(seq[-1]), return len(seq)

    Assume f(seq[no]) < f(seq[no+1]).

    The integer result is useful for indexing the array.

    Args:
        seq: sequence of inputs on which to act
        f: function that returns a comparable when called on any input
        target: value

    Returns: index of item in seq meeting search requirements
    """
    if not seq or f(seq[0]) > target:
        return 0
    elif f(seq[-1]) < target:
        return len(seq)
    upper = len(seq)
    lower = 0
    while (upper - lower) > 1:
        current = (upper + lower) // 2
        next_val = f(seq[current])
        if next_val > target:
            upper = current
        elif next_val <= target:
            lower = current
    return upper


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


def dict_to_tuples(input_dict: Dict[str, str]) -> List[Tuple[str, str]]:
    """Convert a dict to a list of tuples, sorted alphabetically by key.

    Args:
        input_dict (dict): Dictionary to convert

    Returns:
        List of tuples (key, value) sorted alphabetically by key
    """
    return [(key, input_dict[key]) for key in sorted(input_dict.keys())]
