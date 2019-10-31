from aa.pb_tools import dump

def test_raw_lines_to_readable_hex_gives_expected_output():
    input = [b"\x01\x02\x99\xAB\xCD", b"\x03\x04\x76"]
    expected = ["01 02 99 AB CD", "03 04 76"]

    assert dump.raw_lines_to_readable_hex(input) == expected

def test_raw_lines_to_readable_hex_truncates_when_asked():
    input = [b"\x00", b"\x01", b"\x02", b"\x03"]

    max_lines = 2
    expected = ["00", "01"]

    assert dump.raw_lines_to_readable_hex(input, max_lines) == expected