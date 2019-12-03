import os
import logging

from aa.pb_tools import pb_file, types
from aa.pb_tools.validation import PbError

LOG = logging.getLogger(__name__)

def report(message):
    """These will eventually be logs"""
    print(message)


class Action():
    def __init__(self, file_path):
        self.file_path = file_path

    def __str__(self):
        return f"Action on {self.file_path}"


class ChangeType(Action):
    def __init__(self, file_path, new_type):
        super(ChangeType, self).__init__(file_path)
        self.new_type = new_type

    def __str__(self):
        return f"Change type to {self.new_type} " \
            f"on {self.file_path}"


class DontKnow(Action):

    def __str__(self):
        return f"Don't know what to do with file {self.file_path}"



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
        idx = pb_data.payload_info.type
        this_type = idx
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
        LOG.warning("No PB files given")
        return False, {}

    # Create a dict, index type, value a list of filenames which have that type
    files_by_type = group_files_by_type(pb_files)

    if len(files_by_type) == 1:
        # Only one type present in set
        LOG.info("All files have same type")
        return False, files_by_type

    LOG.info("These files don't all have the same type.")

    if len(files_by_type) == 2:
        # Two different types present
        # Work out which has fewest, and how many
        counts = [len(file_list) for file_list in files_by_type.items()]
        list_of_types = files_by_type.values()

        for pb_type, count in zip(files_by_type.keys(), counts):
            LOG.debug(f"   {pb_type}: {count} files")
        if counts[0] == counts[1]:
            LOG.debug(f"-> equal number of mismatched types "
                   f"({counts[0]} files each)")
            return True, files_by_type

        else:
            count_of_smallest = min(counts)
            index_of_smallest = counts.index(min(counts))
            type_of_smallest = list_of_types[index_of_smallest]

            LOG.debug(f"-> least represented type: {type_of_smallest} "
                   f"having {count_of_smallest} files")
            return True, files_by_type



    else:
        LOG.info("More than two types present in list of files.")
        return True, files_by_type


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
            for prefix, filenames_per_pv \
                    in group_filenames_by_prefix(filenames).items():
                full_paths = sorted([
                    os.path.join(this_dir, file)
                    for file in filenames_per_pv
                ])
                # Create the key by joining path to directory with prefix
                # e.g. root/BL13I/OP/MIRR/01/X/RBV
                key = os.path.join(this_dir, prefix)
                pb_files[key] = PbGroup(dir_path=this_dir,
                                        file_paths=full_paths,
                                        prefix=prefix)
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


def join_all_lists_except(exclude_key, orig_dict):
    """
    Given a dict where each value is a list, and one key, return a list
    which is the concatenation of all elements in the dict EXCEPT the given key

    Args:
        exclude_key: Key to exclude
        orig_dict: Starting dictionary; each element must be a list

    Returns:
        List combining all elements except those at exclude_key
    """
    # Make a list of keys without exclude_key in it
    keys = list(orig_dict.keys())
    keys.remove(exclude_key)
    expected_error_types = keys
    # Collect lists from remaining keys
    expected_error_files = []
    for key in expected_error_types:
        expected_error_files.extend(orig_dict[key])
    return expected_error_files


def all_b_within_a(a, b):
    """
    Return true if all elements of b are also in a.

    Args:
        a:  Outer comparison list
        b:  Inner comparison list

    Returns:
        True if all elements of b are also in a, else false
    """
    a = set(a)
    b = set(b)
    return b <= a


class PbGroup():
    """
    Represent a group of PB files for a single PV
    """
    def __init__(self, dir_path, file_paths, prefix):
        self.dir_path = dir_path
        self.file_paths = file_paths
        self.prefix = prefix
        self.pb_files = []
        self.files_by_type = None
        self.actions = []

    def read_files(self):
        """Create a PbFile for each file in self.file_paths"""
        count_files = 0
        for file_path in self.file_paths:
            LOG.debug(f"Create PbFile for {file_path}")
            self.pb_files.append(pb_file.PbFile(file_path))
            count_files += 1
        return count_files

    def check_if_new_type_fixes_errors(self, new_type, only=None):
        """
        Returns True if no type errors found after
        reinterpreting all files with new_type, otherwise False

        Args:
            new_type: Type to try reinterpreting; numerical index
            only: List of file paths to check; must be a subset of
                  self.pb_files

        Returns:
            True if NO ERRORS else False
        """
        total_type_erorrs = 0
        for this_file in self.pb_files:
            if only is not None and this_file.read_path not in only:
                continue
            this_file.decode_raw_lines(requested_type=new_type)
            this_file.check_data_for_errors(
                lazy=True,
                only_check=PbError.EVENT_MISSING_VALUE
            )
            total_type_erorrs += len(this_file.decoding_errors)
        return True if total_type_erorrs == 0 else False

    def check_files_for_type_errors(self):
        """
        Check all files for EVENT_MISSING_VALUE errors and return a list
        of paths to the files which have such errors.

        Returns:
            List of path to files having errors
        """
        count_files_with_errors = 0
        paths_with_type_errors = []

        for this_file, its_path in zip(self.pb_files, self.file_paths):
            LOG.debug("Decode raw lines & check errors")
            #this_file.decode_raw_lines()
            #LOG.debug("Check for errors")
            #this_file.check_data_for_errors(
            #    lazy=True,
            #    only_check=PbError.EVENT_MISSING_VALUE
            #)
            this_file.decode_and_check_lazily()
            if len(this_file.decoding_errors) > 0:
                LOG.info(f"{its_path} has "
                       f"{len(this_file.decoding_errors)} errors")
                count_files_with_errors += 1
                paths_with_type_errors.append(this_file.read_path)

        return paths_with_type_errors

    def suggest_corrective_actions(self):
        """
        Check if any files have type errors, and whether all files have the
        same type in the payload_info.
        Check type of live PV. If all files containing errors have a
        different type from the live PV, try re-interpreting them with the
        live type. If this makes the errors go away, this change is suggested
        as a fix.

        TODO: If live PV check fails, fall back to looking for majority type

        Returns:
        """
        # Check for type errors within individual files
        self.actions = []
        paths_with_type_errors = self.check_files_for_type_errors()
        if len(paths_with_type_errors) == 0:
            LOG.info("No type errors found. No action required.")
            return self.actions

        # Check if type in files has changed
        type_mismatch, self.files_by_type = find_different_type(
            self.pb_files
        )

        if type_mismatch:
            LOG.debug("Trying to determine which type is correct.")

            # Correlate mismatched types with type errors
            live_pv_type = self.check_live_pv_type()
            if live_pv_type is None:
                return

            # Try to correlate with live type
            if live_pv_type in self.files_by_type.keys():
                LOG.debug("Live type matches some of our files.")
                # Make a list of filenames which have different types
                # from the live PV
                paths_with_other_types = join_all_lists_except(
                    exclude_key=live_pv_type,
                    orig_dict=self.files_by_type
                )
                if all_b_within_a(
                    paths_with_other_types,
                    paths_with_type_errors
                ):
                    LOG.info("All the files with errors have types that "
                           "don't match the live PV. ")
                    LOG.debug("Test if they can be reinterpreted with "
                           f"{live_pv_type}:")
                    # Attempt reinterpret
                    new_type = live_pv_type
                    reinterpret_ok = self.check_if_new_type_fixes_errors(
                        new_type
                    )
                    if reinterpret_ok:
                        LOG.info("All error files can be reinterpreted "
                                 "with live type")
                        for file_path in paths_with_type_errors:
                            self.actions.append(
                                ChangeType(file_path,
                                           new_type=new_type)
                            )
                    else:
                        LOG.info("Changing type doesn't fix all type errors")
                        for file_path in paths_with_type_errors:
                            self.actions.append(
                                DontKnow(file_path)
                            )
                else:
                    LOG.info("Files with errors do not all have different "
                             "type from the live PV.")
                    for file_path in paths_with_type_errors:
                        self.actions.append(
                            DontKnow(file_path)
                        )


        return self.actions

    def check_live_pv_type(self):
        """
        Read the PV name from the first PB file. Check what the type is of the
        live PV with that name, using caget.

        Returns:
            A class from aa.epics_event_pb2.TYPE_MAPPINGS or None
        """
        pv_name = self.pb_files[0].payload_info.pvname
        try:
            live_type = types.get_pb_type_of_live_pv(pv_name)
            LOG.debug(f"Type of live PV is {live_type}")
            return live_type
        except ValueError:
            LOG.info("Couldn't get type from live PV")
            return None

    def free_events(self):
        """Try to avoid keeping all events in memory when we are searching
        through large numbers of files"""
        del(self.pb_files)

    def __eq__(self, other):
        return self.dir_path == other.dir_path \
               and self.file_paths == other.file_paths \
               and self.pb_files == other.pb_files

    def __repr__(self):
        return f"PbGroup for dir {self.dir_path} containing {self.file_paths}"


def check_files_in_dir(search_path):
    """Walk the tree at given path. Check each file for errors and report."""
    assert os.path.isdir(search_path)
    LOG.setLevel(level=logging.DEBUG)
    data, total_files = find_all_files_in_tree(
        search_path
    )
    actions = []
    LOG.info(f"Found {total_files} files in this directory.")
    counter = 0
    while len(data) > 0:
        counter += 1
        _, group = data.popitem()
        LOG.info(f"\nChecking {counter}/{total_files} "
                 f"{group.dir_path}/{group.prefix}* :")
        group.read_files()
        actions += group.suggest_corrective_actions()
        #group.free_events()
        del group

    LOG.info(f"\nRead {total_files} total files, "
           f"came up with {len(actions)} actions:")
    for action in actions:
        LOG.info(action)
