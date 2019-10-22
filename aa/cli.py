import argparse
from . import pb_validation

def report_pb_file_errors():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="PB file path", type=str)
    args = parser.parse_args()

    pb_file = pb_validation.PbFile(args.path)
    pb_file.decode_raw_lines()
    pb_file.check_data_for_errors()

    error_count = len(pb_file.decoding_errors)

    if error_count == 0:
        return 0
    else:
        print(f"{error_count} errors found.")
        return 1