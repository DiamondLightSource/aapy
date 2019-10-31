import argparse
from aa.pb_tools import validation

def report_pb_file_errors():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="PB file path", type=str)
    args = parser.parse_args()

    pb_file = validation.PbFile(args.path)
    pb_file.decode_raw_lines()
    pb_file.check_data_for_errors()

    error_count = len(pb_file.decoding_errors)

    if error_count == 0:
        return 0
    else:
        print(f"{error_count} errors found.")
        return 1


def rewrite_pb_header_type():
    parser = argparse.ArgumentParser()
    parser.add_argument("in_path", help="Input PB file path", type=str)
    parser.add_argument("out_path", help="Output PB file path", type=str)
    parser.add_argument("new_type", help="Index of new type to write", type=int)
    args = parser.parse_args()

    pb_file = validation.PbFile(args.in_path)
    pb_file.payload_info.type = int(args.new_type)
    pb_file.write_raw_lines_to_file(args.out_path)
