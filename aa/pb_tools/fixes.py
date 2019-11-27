import os
import logging

from aa.pb_tools import pb_file
from aa.pb_tools.validation import PbError


def report(message):
    """These will eventually be logs"""
    print(message)


def group_files_by_year(pb_files):
    """
    Generate a dictionary with keys of year (from payload info) and values of
    paths to files having that type.

    Args:
        pb_files: List of PbFile objects

    Returns:
        dict of type vs list of paths of files with that type
    """
    files_by_year = {}

    for pb_data in pb_files:
        this_year = pb_data.payload_info.year
        current_list = files_by_year.get(this_year, [])
        current_list.append(pb_data.read_path)
        files_by_year[this_year] = current_list

    return files_by_year


def group_files_by_type(pb_files):
    """
    Generate a dictionary with keys of file type and values of files having
    that type.

    Args:
        pb_files: List of PbFile objects

    Returns:
        dict of type vs list of paths of files with that type
    """
    files_by_type = {}

    for pb_data in pb_files:
        this_type = pb_data.payload_info.type
        current_list = files_by_type.get(this_type, [])
        current_list.append(pb_data.read_path)
        files_by_type[this_type] = current_list

    return files_by_type


def find_different_type(pb_files):
    """
    Given a list of files, determine if there are any that are a different type

    Args:
        pb_files: list of PbFile objects

    Returns:
        type mismatch (bool) files by type (dict)
    """

    if len(pb_files) == 0:
        report("No PB files given")
        return {}

    # Create a dict, index type, value a list of filenames which have that type
    files_by_type = group_files_by_type(pb_files)

    if len(files_by_type) == 1:
        # Only one type present in set
        report("All files have same type")
        return False, files_by_type

    report("These files don't all have the same type.")

    if len(files_by_type) == 2:
        # Two different types present
        # Work out which has fewest, and how many
        counts = [len(file_list) for file_list in files_by_type.items()]
        types = files_by_type.values()
        if counts[0] == counts[1]:
            report(f"- equal number of mismatched types "
                   f"({counts[0]} files each)")
            return

        else:
            count_of_smallest = min(counts)
            index_of_smallest = counts.index(min(counts))
            type_of_smallest = types[index_of_smallest]

            report(f"- least represented type: {type_of_smallest} "
                   f"having {count_of_smallest} files")
            return

    else:
        report("More than two types present in list of files. "
               "Can't make a sensible deduction about which is right.")
        return


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
    for this_dir, _, filenames in os.walk(root_dir):
        if len(filenames) > 0:
            for prefix, filenames_per_pv in group_filenames_by_prefix(filenames).items():
                full_paths = sorted([
                    os.path.join(this_dir, file)
                    for file in filenames_per_pv
                ])
                # Create the key by joining path to directory with prefix
                # e.g. root/BL13I/OP/MIRR/01/X/RBV
                key = os.path.join(this_dir, prefix)
                pb_files[key] = PbGroup(dir_path=this_dir,
                                        file_paths=full_paths)
                total_count += len(full_paths)

    return pb_files, total_count


def group_filenames_by_prefix(list_of_filenames):
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
        self.files_by_type = None

    def read_files(self):
        count_files = 0
        for file_path in self.file_paths:
            self.pb_files.append(pb_file.PbFile(file_path))
            count_files += 1
        return count_files

    def check_files_for_errors(self):

        # Errors within individual files
        count_files_with_errors = 0
        for this_file, its_path in zip(self.pb_files, self.file_paths):
            this_file.decode_raw_lines()
            this_file.check_data_for_errors(lazy=True)
            if len(this_file.decoding_errors) > 0:
                report(f"{its_path} has "
                       f"{len(this_file.decoding_errors)} errors")
                count_files_with_errors += 1

        # Check for non-matching types
        type_mismatch, self.files_by_type = find_different_type(
            self.pb_files
        )

        if type_mismatch:
            # Correlate mismatched types with type errors
            files_with_type_errors = []
            for this_file in self.pb_files:
                if PbError.EVENT_MISSING_VALUE in this_file.parsing_errors:
                    index = self.pb_files.index(this_file)
                    files_with_type_errors.append(index)

            if len(files_with_type_errors):
                pass


        return count_files_with_errors

    def free_events(self):
        """Try to avoid keeping all events in memory when we are searching
        through large numbers of files"""
        del(self.pb_files)

    def __eq__(self, other):
        """TODO: Decide whether comparing pb_files is a valid check"""
        return self.dir_path == other.dir_path \
               and self.file_paths == other.file_paths \
               and self.pb_files == other.pb_files

    def __repr__(self):
        return f"PbGroup for dir {self.dir_path} containing {self.file_paths}"


def demo(search_path = None):
    """Walk the tree at given path. Check each file for errors and report."""
    if search_path is None:
        search_path = "/dls/science/users/tdq39642/aa/lts_one_device/"
    logging.basicConfig(level=logging.WARN)
    data, total_files = find_all_files_in_tree(
        search_path
    )
    total_with_errors = 0
    report(f"Found {total_files} files in this directory.")
    for _, group in data.items():
        report(f"Checking files in {group.dir_path}:")
        group.read_files()
        total_with_errors += group.check_files_for_errors()
        group.free_events()

    report(f"Read {total_files} total files, "
           f"found {total_with_errors} have errors")
