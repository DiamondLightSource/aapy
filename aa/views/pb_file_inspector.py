import sys
from PyQt5.QtWidgets import QMainWindow, QApplication
from PyQt5 import uic
from pathlib import Path


UIC_DIRECTORY = "ui"


def get_uic_obj(ui_file_name):
    """Return a reference to a UIC object from a qt designer *.ui generated file.

    Args:
        ui_file_name: The name of the .ui file

    Returns:
        QtObject: Usually a QtDialog reference from the .ui file for burtinter.
    """
    # Root directory of burtinter application
    aapy_root = Path(__file__).resolve().parents[1]

    ui_file_reference = aapy_root / UIC_DIRECTORY / ui_file_name

    return uic.loadUi(ui_file_reference)


class PbFileBrowser(object):

    def __init__(self):
        self.window = QMainWindow(None)

        self.ui = get_uic_obj("pb_file_inspector.ui")
        form = self.ui

        self.ui.show()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    _ = PbFileBrowser()
    sys.exit(app.exec_())