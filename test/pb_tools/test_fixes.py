import os
from pytest import mark
from aa import pb
from aa import epics_event_pb2 as ee
from aa.pb_tools import fixes, pb_file

def test_split_files_by_prefix_gives_correct_output():
    test_filenames = [
        "OPS:2017.pb",
        "OPS:2018.pb",
        "OPS:2019.pb",
        "STAT:2017.pb",
        "STAT:2018.pb",
        "STAT:2019.pb",
    ]
    output = fixes.group_filenames_by_prefix(test_filenames)
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
    output, count_found = fixes.find_all_files_in_tree(test_dir)

    base_path = os.path.join(test_dir, "BL13I", "OP", "MIRR", "01")
    expect = {
        os.path.join(base_path, "X", "RBV"): fixes.PbGroup(
            dir_path=os.path.join(base_path, "X"),
            file_paths=sorted(
                [
                    os.path.join(base_path, "X", "RBV:2018.pb"),
                    os.path.join(base_path, "X", "RBV:2019.pb"),
                ]
            ),
            prefix=os.path.join(base_path, "X", "RBV"),
        ),
        os.path.join(base_path, "STAT"): fixes.PbGroup(
            dir_path = base_path,
            file_paths = sorted(
                [
                    os.path.join(base_path, "STAT:2018.pb"),
                    os.path.join(base_path, "STAT:2019.pb"),
                ]
            ),
            prefix=os.path.join(base_path, "STAT")
        ),

    }
    expect_count = 4
    assert output == expect
    assert count_found == expect_count


def test_group_files_by_year_gives_expected_output():

    filenames = []
    pb_files = []
    for year in [2017, 2018, 2018, 2019]:
        test_file = pb_file.PbFile()
        test_file.payload_info = ee.PayloadInfo(
            year=year,
            type=5,
            pvname="BL14J-PS-SHTR-03:OPS",
            elementCount=1,
        )
        pb_files.append(test_file)
        suffix = ""
        while f"{year}.pb{suffix}" in filenames:
            suffix += "1"
        this_filename = f"{year}.pb{suffix}"
        test_file.read_path = this_filename
        filenames.append(this_filename)


    result = fixes.group_files_by_year(pb_files)

    expected = {
        2017: ["2017.pb"],
        2018: ["2018.pb",
               "2018.pb1"],
        2019: ["2019.pb"],
    }

    assert result == expected


def test_group_files_by_type_gives_expected_output():

    filenames = []
    pb_files = []
    for type in [1, 2, 3, 3, 4]:
        test_file = pb_file.PbFile()
        year = 2000 + type
        test_file.payload_info = ee.PayloadInfo(
            year=2000 + type,
            type=type,
            pvname="BL14J-PS-SHTR-03:OPS",
            elementCount=1,
        )
        pb_files.append(test_file)
        suffix = ""
        while f"{year}.pb{suffix}" in filenames:
            suffix += "1"
        this_filename = f"{year}.pb{suffix}"
        test_file.read_path = this_filename
        filenames.append(this_filename)

    result = fixes.group_files_by_type(pb_files)

    expected = {
        1: ["2001.pb"],
        2: ["2002.pb"],
        3: ["2003.pb", "2003.pb1"],
        4: ["2004.pb"],
    }

    assert result == expected


def test_join_all_lists_except_gives_correct_output():
    test_dict = {
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9],
    }
    exclude_key = "b"
    expected_output = [1, 2, 3, 7, 8, 9]
    result = fixes.join_all_lists_except(exclude_key, test_dict)
    assert result == expected_output

@mark.parametrize("a,b,expect",[
    ([1,2,3], [3,2,1], True),
    ([1,2,3], [4,3,1], False),
    ([1,2,3], [1,2], True),
    ([1,2,3,4,5,6], [5,4,3], True),
    ([1,2,3,4,5,6], [4,5,7], False)
])
def test_all_b_within_a_gives_correct_result(a, b, expect):
    assert fixes.all_b_within_a(a, b) == expect


def test_creating_list_of_actions():
    """Just makes sure these classes can be instantiated with
    correct signature"""
    action_list = [
        fixes.ChangeType("/file/path/1", new_type=4),
        fixes.DontKnow("/file/path/2"),
    ]

def test_find_different_type_with_all_same():
    type_used = 5

    test_files = []
    for idx in range(10):
        new_file = pb_file.PbFile()
        new_file.read_path = f"File {idx}"
        new_file.payload_info = ee.PayloadInfo(
            year=2017,
            type=type_used,
            pvname="BL14J-PS-SHTR-03:OPS",
            elementCount=1,
        )
        test_files.append(new_file)


    type_mismatch, files_by_type = fixes.find_different_type(test_files)

    assert type_mismatch == False

    assert files_by_type == {
        type_used: [this_file.read_path for this_file in test_files]
    }

def test_find_different_type_with_two_equal_types():
    type_used = 5

    test_files = []
    files_five = []
    files_six = []
    for idx in range(10):
        new_file = pb_file.PbFile()
        new_file.read_path = f"File {idx}"
        new_file.payload_info = ee.PayloadInfo(
            year=2017,
            type=type_used,
            pvname="BL14J-PS-SHTR-03:OPS",
            elementCount=1,
        )

        if idx % 2 == 0:
            files_five.append(new_file)
        else:
            new_file.payload_info.type = 6
            files_six.append(new_file)
        test_files.append(new_file)

    print(test_files)

    print([f.payload_info.type for f in test_files])

    type_mismatch, files_by_type = fixes.find_different_type(test_files)

    assert type_mismatch == True

    assert files_by_type == {
        5: [this_file.read_path for this_file in files_five],
        6: [this_file.read_path for this_file in files_six],
    }

def test_find_different_type_with_two_unequal_types():
    type_used = 5

    test_files = []
    files_five = []
    files_six = []
    for idx in range(10):
        new_file = pb_file.PbFile()
        new_file.read_path = f"File {idx}"
        new_file.payload_info = ee.PayloadInfo(
            year=2017,
            type=type_used,
            pvname="BL14J-PS-SHTR-03:OPS",
            elementCount=1,
        )

        if idx % 3 == 0:
            files_five.append(new_file)
        else:
            new_file.payload_info.type = 6
            files_six.append(new_file)
        test_files.append(new_file)

    print(test_files)

    print([f.payload_info.type for f in test_files])

    type_mismatch, files_by_type = fixes.find_different_type(test_files)

    assert type_mismatch == True

    assert files_by_type == {
        5: [this_file.read_path for this_file in files_five],
        6: [this_file.read_path for this_file in files_six],
    }


def test_find_different_types_given_nothing_returns_nothing():
    type_mismatch, files_by_type = fixes.find_different_type([])
    assert type_mismatch == False

    assert files_by_type == {}


def test_count_elems_of_sub_lists_gives_correct_output():
    test_dict = {idx: [i for i in range(idx)] for idx in range(4, -1, -1)}
    expect = [(i,i) for i in range(5)]

    assert fixes.count_elemns_of_sub_lists(test_dict) == expect