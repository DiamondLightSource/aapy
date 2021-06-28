# flake8: noqa

from aa import *


def test_module_import_star():
    # Check a random selection of things exist in the namespace
    # from the start import above
    assert all([SCAN, MONITOR, js, pb, data, fetcher, utils])
