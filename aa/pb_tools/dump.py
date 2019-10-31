from aa.pb_tools import pb_file


def dump_pb_data(input_path, show_payload_info=True, show_binary=True, show_events=False):

    file = pb_file.PbFile(input_path)

    if show_payload_info:
        print(repr(file.payload_info))

    if show_binary:
        output_lines = raw_lines_to_readable_hex(file.raw_lines)
        for line in output_lines:
            print(line)

    if show_events:
        file.decode_raw_lines()
        for event in file.pb_events:
            print(repr(event))



def raw_lines_to_readable_hex(input_lines, max_lines=None):
    """
    Format a list of lines of binary data into 2-digit hex that can be read
    by a human.

    The format is kept simple so that the output can be passed
    straightforwardly e.g. to diff or displayed in another program.

    Args:
        input_lines: list containing lines
        max_lines: Truncate output after this many lines

    Returns:
        List of strings containing formatted lines
    """
    output_lines = []

    # Count lines
    counter = 0

    for line in input_lines:
        chars = []

        for byte in line:
            # 2 characters uppercase hex, 0-padded
            chars.append("{:>02X}".format(byte))

        # Do it this way to avoid trailing whitespace
        output_lines.append(" ".join(chars))

        counter += 1

        if max_lines and counter >= max_lines:
            break

    return output_lines