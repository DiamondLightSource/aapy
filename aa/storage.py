
STORAGE_PATHS = {"LTS": "/dls/science/users/tdq39642/aa/mock_lts"}

class ParsingError(Exception):
    pass

def pv_name_from_path(path):
    """Given a path relative to storage root, infer
    what the PV name is"""
    before_colon = path.split(":")[0]
    path_as_list = before_colon.split("/")

    if len(path_as_list) < 4:
        raise ParsingError("Path too short")
    pv_stem = "-".join(path_as_list[0:4])

    if len(path_as_list) > 4:
        pv_name = ":".join([pv_stem] + path_as_list[4:])
    else:
        pv_name = pv_stem

    return pv_name


#def list_pvs_in_storage(storage_type):