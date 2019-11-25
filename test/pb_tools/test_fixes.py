from aa.pb_tools import fixes

def test_split_files_by_prefix_gives_correct_output():
    test_filenames = [
        "OPS:2017.pb",
        "OPS:2018.pb",
        "OPS:2019.pb",
        "STAT:2017.pb",
        "STAT:2018.pb",
        "STAT:2019.pb",
    ]
    output = fixes.separate_by_prefix(test_filenames)
    expected_output = {
        "OPS":
            ["OPS:2017.pb",
            "OPS:2018.pb",
            "OPS:2019.pb",],
        "STAT":
            ["STAT:2017.pb",
            "STAT:2018.pb",
            "STAT:2019.pb",],
    }
    assert output == expected_output