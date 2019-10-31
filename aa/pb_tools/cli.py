import argparse
import click
from aa.pb_tools import validation


@click.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.help_option('--help', '-h')
def report_pb_file_errors(input_path):
    pb_file = validation.PbFile(input_path)
    pb_file.decode_raw_lines()
    pb_file.check_data_for_errors()

    error_count = len(pb_file.decoding_errors)

    if error_count == 0:
        return 0
    else:
        print(f"{error_count} errors found.")
        return 1


@click.command()
@click.argument("in_path", type=click.Path(exists=True))
@click.argument("out_path", type=click.Path(exists=False))
@click.argument("new_type", type=click.IntRange(min=0, max=15, clamp=False))
@click.help_option('--help', '-h')
def rewrite_pb_header_type(in_path, out_path, new_type):
    pb_file = validation.PbFile(in_path)
    pb_file.payload_info.type = int(new_type)
    pb_file.write_raw_lines_to_file(out_path)
