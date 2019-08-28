import pytest
from aa import storage

@pytest.mark.parametrize("path, pv", [
    ("BL13J/MO/PI/01/X:2018.pb", "BL13J-MO-PI-01:X"),
    ("BL14I/VA/IONP/14/SP1OFF:2015.pb", "BL14I-VA-IONP-14:SP1OFF"),
    ("BL14I/MO/STAGE/01/ROT/FERROR:2017.pb", "BL14I-MO-STAGE-01:ROT:FERROR"),
    ("BL14I/MO/STAGE/01/XF.RBV:2019.pb", "BL14I-MO-STAGE-01:XF.RBV")
])
def test_pv_name_from_path(path, pv):
    assert storage.pv_name_from_path(path) == pv