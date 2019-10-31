import argparse
import click
from aa.pb_tools import validation, dump
from aa.pb_tools.views import pb_file_inspector

@click.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.help_option('--help', '-h')
def report_pb_file_errors(input_path):
    pb_file = validation.PbFile(input_path)
    pb_file.decode_raw_lines()
    pb_file.check_data_for_errors()

    error_count = len(pb_file.decoding_errors)

    if error_count == 0:
        print(f"No errors found.")
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


@click.command(help="A graphical application to browse and "
                    "fix protocol buffer files.")
@click.option("--input_file",
              type=click.Path(exists=True),
              help="Load this file on startup",
              default=None)
@click.help_option('--help', '-h')
def invoke_pb_file_inspector(input_file):
    pb_file_inspector.invoke(input_file)


@click.command(help="Print a plain text representation of the data in the "
                    "protocol buffer file INPUT_FILE to standard output. "
                    "By default, the payload info and interpreted events "
                    "are shown. A representation of the raw bytes may be "
                    "given using --binary. The output may be passed to "
                    "programs such as diff for further processing.")
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--payload-info/--no-payload-info", default=True,
              show_default=True,
              help="Show payload info.")
@click.option("--events/--no-events", default=True,
              show_default=True,
              help="Show decoded events.")
@click.option("--binary/--no-binary", default=False,
              show_default=True,
              help="Show representation of raw bytes")
@click.help_option('--help', '-h')
def dump_pb_data(payload_info, input_file, events, binary):
    dump.dump_pb_data(input_file, payload_info, binary, events)