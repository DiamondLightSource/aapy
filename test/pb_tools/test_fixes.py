import os
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


def test_find_all_files_in_tree_gives_expected_output():
    # Build absolute path to our test directory from the relative path
    test_dir = os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "data",
            "mock_lts"
        )
    )
    output = fixes.find_all_files_in_tree(test_dir)

    base_path = os.path.join(test_dir, "BL13I", "OP", "MIRR", "01")
    expect = {
        os.path.join(base_path, "X", "RBV"): fixes.PbGroup(
            dir_path=os.path.join(base_path, "X"),
            file_paths=[
                os.path.join(base_path, "X", "RBV:2018.pb"),
                os.path.join(base_path, "X", "RBV:2019.pb"),
            ]),
        os.path.join(base_path, "STAT"): fixes.PbGroup(
            dir_path = base_path,
            file_paths = [
                os.path.join(base_path, "STAT:2018.pb"),
                os.path.join(base_path, "STAT:2019.pb"),
            ]),

    }
    assert output == expect