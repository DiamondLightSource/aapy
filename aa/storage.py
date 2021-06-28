__all__ = [
    "ParsingError",
    "pv_name_from_path",
]


class ParsingError(Exception):
    pass


def pv_name_from_path(path):
    """Given a path to a PB file relative to storage root, infer what the
    PV name is.

    In the AA storage, a PV name is broken up into parts
    and there are subdirectories with each part of the name.
    This method is to reverse that process so you can work out
    what a PV name is from the path to a PB file.

    The first three slashes in the path become dashes in the
    PV name. Subsequent slashes become colons.

    E.g. "BL14I/MO/STAGE/01/XF.RBV:2019.pb" goes to
    "BL14I-MO-STAGE-01:XF.RBV"
    """
    # pattern = re.compile(r"((?:(?:[\w/]+)+)(?:.\w+)?):(\w+\.pb)")
    # Strip relative specifier from beginning
    if path[0] == ".":
        path = path[2:]

    split_by_dot = path.split(".")
    # There will be one dot if PV does not specify a field
    # otherwise two
    if len(split_by_dot) not in [2, 3] or split_by_dot[-1] != "pb":
        raise ParsingError("Does not look like a protobuf file")

    # Strip off the trailing part of the file name
    # e.g. year
    before_colon = path.split(":")[0]
    path_as_list = before_colon.split("/")

    # Check that PV name matches convention in that it has at least 4 parts:
    # AA-BB-CC-DD[:EE:FF...]
    if len(path_as_list) < 4:
        raise ParsingError("Path too short")

    # The first four parts are joined by dashes
    pv_stem = "-".join(path_as_list[0:4])

    # Subsequent parts are joined by colons
    if len(path_as_list) > 4:
        pv_name = ":".join([pv_stem] + path_as_list[4:])
    else:
        pv_name = pv_stem

    return pv_name
