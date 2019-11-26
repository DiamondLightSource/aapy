"""Command-line tools for working with protocol buffer files"""
import click
from aa.pb_tools import validation, pb_file, dump
from aa.pb_tools.views import pb_file_inspector

@click.command()
@click.argument("input_paths", type=click.Path(exists=True), nargs=-1,
                required=True)
@click.option("-v", "--verbose", is_flag=True)
@click.option("-t", "--terse", is_flag=True)
@click.help_option('--help', '-h')
def report_pb_file_errors(input_paths, verbose, terse):
    """
    Check for known types of errors in the protocol buffer
    file(s) at INPUT_PATHS. Prints how many errors in each
    file. In verbose mode, also prints a list of errors.
    In terse mode, prints only files with errors
    Returns 0 if no errors in any file, else 1.
    """

    if verbose and terse:
        click.echo("Can't be both --verbose and --terse", err=True)
        return 2

    return_value = 0
    for input_path in input_paths:
        pb_data = pb_file.PbFile(input_path)
        pb_data.decode_raw_lines()
        pb_data.check_data_for_errors()

        error_count = len(pb_data.decoding_errors)

        # Print results to stdout. Skip in terse mode if no errors
        if error_count > 0 or not terse:
            print(f"{input_path}: "
                  f"{error_count} error{'' if error_count == 1 else 's'}")

        if error_count == 0:
            return_value = return_value | 0
        else:
            # Print list of errors in verbose mode
            if verbose:
                for index, error_type in pb_data.decoding_errors:
                    error_string = validation.PB_ERROR_STRINGS[error_type]
                    print(f"{error_string} at index {index}")

            return_value = return_value | 1
    return return_value


@click.command()
@click.argument("in_path", type=click.Path(exists=True))
@click.argument("out_path", type=click.Path(exists=False))
@click.argument("new_type", type=click.IntRange(min=0, max=15, clamp=False))
@click.help_option('--help', '-h')
def rewrite_pb_header_type(in_path, out_path, new_type):
    """
    Modify the type in the payload info for the protocol
    buffer file at IN_PATH, saving a new file to OUT_PATH
    without attempting to decode and encode the events.
    Therefore there is no guarantee that the new type is the
    correct one for these events, and we don't try to
    convert the type of the events either.
    The new type has the index NEW_TYPE which is
    an integer type index from the following list:

        0: ee.ScalarString,
        1: ee.ScalarShort,
        2: ee.ScalarFloat,
        3: ee.ScalarEnum,
        4: ee.ScalarByte,
        5: ee.ScalarInt,
        6: ee.ScalarDouble,
        7: ee.VectorString,
        8: ee.VectorShort,
        9: ee.VectorFloat,
        10: ee.VectorEnum,
        11: ee.VectorChar,
        12: ee.VectorInt,
        13: ee.VectorDouble,
        14: ee.V4GenericBytes
    """
    # Load raw lines
    pb_data = pb_file.PbFile(in_path)
    # Modify the type in the payload info
    pb_data.payload_info.type = int(new_type)
    # Write the raw lines to the file with the updated payload info,
    # wihtout trying to decode / encode the events
    pb_data.write_raw_lines_to_file(out_path)


@click.command()
@click.argument("input-file",
                type=click.Path(exists=True),
                required=False,
                nargs=1)
@click.help_option('--help', '-h')
def invoke_pb_file_inspector(input_file):
    """
    A graphical application to browse and
    fix protocol buffer files.
    """
    pb_file_inspector.invoke(input_file)


@click.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option("--payload-info/--no-payload-info", default=True,
              show_default=True,
              help="Show payload info.")
@click.option("--events/--no-events", default=False,
              show_default=True,
              help="Show decoded events.")
@click.option("--binary/--no-binary", default=False,
              show_default=True,
              help="Show representation of raw bytes")
@click.help_option('--help', '-h')
def dump_pb_data(payload_info, input_path, events, binary):
    """
    Print a plain text representation of the data in the
    protocol buffer file at INPUT_PATH to standard output.
    By default, the payload info and number of events
    are shown. A representation of the raw bytes may be
    given using --binary. The interpreted events may be
    listed using --events. The output may be passed to
    programs such as diff for further processing.
    """
    pb_data = pb_file.PbFile(input_path)

    if payload_info:
        print(repr(pb_data.payload_info))
        event_count = len(pb_data.raw_lines)
        print(f"File contains {event_count} events")

    if binary:
        output_lines = dump.raw_lines_to_readable_hex(pb_data.raw_lines)
        for line in output_lines:
            print(line)

    if events:
        pb_data.decode_raw_lines()
        for event in pb_data.pb_events:
            print(repr(event))
