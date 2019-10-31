import click
from aa.pb_tools import validation, pb_file, dump
from aa.pb_tools.views import pb_file_inspector

@click.command(help="Check for known types of errors in the protocol buffer "
                    "file at INPUT_PATH. Returns 0 if there are no errors, "
                    "otherwise prints a list of errors to standard out "
                    "and returns 1.")
@click.argument("input_path", type=click.Path(exists=True))
@click.help_option('--help', '-h')
def report_pb_file_errors(input_path):
    file = pb_file.PbFile(input_path)
    file.decode_raw_lines()
    file.check_data_for_errors()

    error_count = len(file.decoding_errors)

    if error_count == 0:
        print(f"No errors found.")
        return 0
    else:
        for index, error_type in file.decoding_errors:
            error_string = validation.PB_ERROR_STRINGS[error_type]
            print(f"{error_string} at index {index}")

        print(f"Total: {error_count} errors.")

        return 1


@click.command()
@click.argument("in_path", type=click.Path(exists=True))
@click.argument("out_path", type=click.Path(exists=False))
@click.argument("new_type", type=click.IntRange(min=0, max=15, clamp=False))
@click.help_option('--help', '-h')
def rewrite_pb_header_type(in_path, out_path, new_type):
    file = pb_file.PbFile(in_path)
    file.payload_info.type = int(new_type)
    file.write_raw_lines_to_file(out_path)


@click.command(help="A graphical application to browse and "
                    "fix protocol buffer files.")
@click.option("--input-file",
              type=click.Path(exists=True),
              help="Load this file on startup",
              default=None)
@click.help_option('--help', '-h')
def invoke_pb_file_inspector(input_file):
    pb_file_inspector.invoke(input_file)


@click.command(help="Print a plain text representation of the data in the "
                    "protocol buffer file at INPUT_PATH to standard output. "
                    "By default, the payload info and interpreted events "
                    "are shown. A representation of the raw bytes may be "
                    "given using --binary. The output may be passed to "
                    "programs such as diff for further processing.")
@click.argument("input_path", type=click.Path(exists=True))
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
def dump_pb_data(payload_info, input_path, events, binary):
    file = pb_file.PbFile(input_path)

    if payload_info:
        print(repr(file.payload_info))

    if binary:
        output_lines = dump.raw_lines_to_readable_hex(file.raw_lines)
        for line in output_lines:
            print(line)

    if events:
        file.decode_raw_lines()
        for event in file.pb_events:
            print(repr(event))
