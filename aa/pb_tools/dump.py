"""
Tools to format protobuf file data in a human readable format.
"""


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
