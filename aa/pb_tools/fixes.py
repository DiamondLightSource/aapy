import os
import logging
from aa.pb_tools import pb_file

def main():
    # List of input file paths
    root_path = "/mnt/archiver1/lts/BL14J/PS/SHTR/03"
    input_files = [os.path.join(root_path, filename) for filename in [
        "OPS:2017.pb",
        "OPS:2018.pb",
        "OPS:2019.pb",
    ]]
    input_files.append("/dls/science/users/tdq39642/aa/OPS:2017.pb")

    pb_files = {filename: pb_file.PbFile(filename) for filename in input_files}

    for filename, pb_data in pb_files.items():
        pb_data.decode_raw_lines()
        pb_data.check_data_for_errors()

    # If one file has a different type in the payload info then it
    # might be wrong, suggest changing to match the others;
    # Can check if changing to match the others would get rid of all
    # missing value erorrs in that file
    minority, majority, odd_ones_out = find_different_type(pb_files)
    report(f"Filenames retunred: {odd_ones_out}")


def report(message):
    """These will eventually be logs"""
    print(message)


def find_different_type(pb_files):
    """
    Given a list of files, determine if there are any that are a different type

    Args:
        pb_files:

    Returns:

    """

    if len(pb_files) == 0:
        report("No PB files given")
        return None, None, []

    if len(pb_files) == 2:
        report("With only two input files, it's not possible to tell "
               "which one is right and which wrong.")
        return None, None, []

    # Create a dict, index type, value a list of filenames which have that type
    files_by_type = {}

    for filename, pb_data in pb_files.items():
        this_type = pb_data.payload_info.type
        current_list = files_by_type.get(this_type, [])
        current_list.append(filename)
        files_by_type[this_type] = current_list

    if len(files_by_type) == 1:
        # Only one type present in set
        report("All files have same type")
        return None, None, []
    if len(files_by_type) == 2:
        # Two different types present
        # Work out which has fewest, and how many
        index = 0
        if len(files_by_type[0]):
            pass #incomplete

        report(f"Least represented type: {minority} having {min_count} files")
        return None, None, files_by_type[minority]
    else:
        report("More than two types present in list of files, "
               "can't make a sensible deduction about which is right.")
        return None, None, []


def find_all_files_in_tree(root_dir):
    """
    Walk the tree under root dir, populating a dict containing PbGroup objects
    with full paths to all files found.

    Args:
        root_dir: The root directory to walk

    Returns:
        dict of Dir objects containing a list of the filenames in each
    """
    pb_files = {}
    total_count = 0
    for this_dir, subdirs, filenames in os.walk(root_dir):
        if len(filenames) > 0:
            for prefix, filenames_per_pv in separate_by_prefix(filenames).items():
                full_paths = [
                    os.path.join(this_dir, file)
                    for file in filenames_per_pv
                ]
                # Create the key by joining path to directory with prefix
                # e.g. root/BL13I/OP/MIRR/01/X/RBV
                key = os.path.join(this_dir, prefix)
                pb_files[key] = PbGroup(dir_path=this_dir,
                                        file_paths=full_paths)
                total_count += len(full_paths)

    return pb_files, total_count


def separate_by_prefix(list_of_filenames):
    """
    Returns a dict with the supplied filenames split by the prefix (before
    the colon)

    Args:
        list_of_filenames:

    Returns:
        Dict. Key: prefix; value: list of filenames with that prefix
    """
    result = {}
    for filename in list_of_filenames:
        prefix, _ = filename.split(":")
        if prefix not in result:
            result[prefix] = [filename]
        else:
            result[prefix] = result[prefix] + [filename]
    return result


class PbGroup():
    """
    Represent a group of PB files for a single PV
    """
    def __init__(self, dir_path, file_paths):
        self.dir_path = dir_path
        self.file_paths = file_paths
        self.pb_files = []

    def read_files(self):
        count_files = 0
        for file_path in self.file_paths:
            self.pb_files.append(pb_file.PbFile(file_path))
            count_files += 1
        return count_files

    def check_files_for_errors(self):
        count_files_with_errors = 0
        for this_file, its_path in zip(self.pb_files, self.file_paths):
            this_file.decode_raw_lines()
            this_file.check_data_for_errors(lazy=True)
            if len(this_file.decoding_errors) > 0:
                report(f"{its_path} has "
                       f"{len(this_file.decoding_errors)} errors")
                count_files_with_errors += 1
        return count_files_with_errors

    def free(self):
        del(self.pb_files)

    def __eq__(self, other):
        """TODO: Decide whether comparing pb_files is a valid check"""
        return self.dir_path == other.dir_path \
               and self.file_paths == other.file_paths \
               and self.pb_files == other.pb_files

    def __repr__(self):
        return f"PbGroup for dir {self.dir_path} containing {self.file_paths}"


def demo(search_path = None):
    if search_path is None:
        search_path = "/dls/science/users/tdq39642/aa/lts_one_device/"
    logging.basicConfig(level=logging.WARN)
    data, total_files = find_all_files_in_tree(
        search_path
    )
    total_with_errors = 0
    report(f"Founf {total_files} files in this directory.")
    for key, group in data.items():
        group.read_files()
        total_with_errors += group.check_files_for_errors()
        group.free()

    report(f"Read {total_files} total files, "
           f"found {total_with_errors} have errors")
