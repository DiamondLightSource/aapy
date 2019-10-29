import sys
import os
from PyQt5.QtWidgets import QMainWindow, QApplication
from PyQt5 import uic
from pathlib import Path

from aa import pb_validation

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
        form.read_file_button.clicked.connect(self.load_pb_file)
        self.ui.show()
        self.pb_file = pb_validation.PbFile()

    def load_pb_file(self):
        input_path = self.ui.input_file_path.text()
        self.ui.status_box.setText(f"Loading file {input_path}")
        try:
            # Check file exists
            if not os.path.exists(input_path):
                self.ui.status_box.setText(f"No such file: {input_path}")
                return False
            # Read file, getting payload_info
            self.pb_file.read_raw_lines_from_file(input_path)
            # Populate header info on GUI
            self.ui.pv_name_control.setText(
                self.pb_file.payload_info.pvname
            )
            self.ui.year_control.setText(
                str(self.pb_file.payload_info.year)
            )
            self.ui.data_type_control.setCurrentIndex(
                self.pb_file.payload_info.type
            )
            self.ui.element_count_control.setValue(
                self.pb_file.payload_info.elementCount
            )
        except Exception as e:
            self.ui.status_box.setText(f"Exception reading header: {e}")
            return False
        else:
            self.ui.status_box.setText("Loaded header")

    def decode_events_and_check(self):
        if self.pb_file.payload_info is None:
            self.ui.status_box.setText(f"Need to load a file first")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    _ = PbFileBrowser()
    sys.exit(app.exec_())