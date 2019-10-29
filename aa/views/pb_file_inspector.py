import sys
import os
from PyQt5.QtWidgets import QMainWindow, QApplication, QTableWidgetItem
from PyQt5 import uic
from PyQt5.QtGui import QColor
from pathlib import Path
import datetime
import pytz
import logging

from aa import pb_validation, pb

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
        form.decode_button.clicked.connect(self.decode_events_and_check)
        self.ui.show()
        self.pb_file = pb_validation.PbFile()
        self.reset()

    def reset(self):
        """Restore empty / default values on all form widgets"""
        self.ui.input_file_path.setText("")
        self.ui.events_table.setRowCount(0)
        self.ui.events_table.setColumnCount(4)
        self.ui.pv_name_control.setText("")
        self.ui.year_control.setText("")
        self.ui.data_type_control.setCurrentIndex(0)
        self.ui.element_count_control.setValue(1)
        self.ui.status_box.setText("")

    def load_pb_file(self):
        input_path = self.ui.input_file_path.text()
        self.set_status(f"Loading file {input_path}")
        try:
            # Check file exists
            if not os.path.exists(input_path):
                self.set_status(f"No such file: {input_path}")
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
            self.set_status(f"Exception reading header: {e}")
            return False
        else:
            self.set_status("Loaded header")

    def set_header_from_form(self):
        # Populate header info on GUI
        self.pb_file.payload_info.pvname = self.ui.pv_name_control.text()
        self.pb_file.payload_info.year = int(self.ui.year_control.text())
        self.pb_file.payload_info.type = self.ui.data_type_control.currentIndex()
        self.pb_file.payload_info.elementCount = self.ui.element_count_control.value()

    def set_status(self, status_string):
        self.ui.status_box.setText(status_string)

    def decode_events_and_check(self):
        if self.pb_file.payload_info is None:
            self.set_status(f"Need to load a file first")
            return

        # Apply settings from modified header
        self.set_status("Applying updated payload info")
        self.set_header_from_form()

        # Remove any existing cells from table
        self.ui.events_table.setRowCount(0)

        # Decode events using updated header
        self.set_status("Decoding events")
        self.pb_file.decode_raw_lines()

        # Check for errors
        self.set_status("Checking for errors")
        self.pb_file.check_data_for_errors()

        # Update table cells
        rows = len(self.pb_file.pb_events)
        self.ui.events_table.setRowCount(rows)

        # Arrange errors by row
        error_list = [[]] * rows
        self.ui.errors_list.clear()
        for index, error in self.pb_file.decoding_errors:
            error_list[index].append(error)
            error_string = pb_validation.PB_ERROR_STRINGS[error]
            self.ui.errors_list.addItem(
                f"{error_string} at {index}"
            )
        if len(self.pb_file.decoding_errors) <= 1:
            self.ui.errors_list.addItem("No errors found")

        # Display events and errors
        row = 0
        for event in self.pb_file.pb_events:
            timestamp = pb.event_timestamp(self.pb_file.payload_info.year,
                                           event)
            timezone = pytz.timezone("Europe/London")
            legible_timestamp = datetime.datetime.fromtimestamp(timestamp,
                                                                    timezone).isoformat()

            timestamp_cell = QTableWidgetItem(str(
                legible_timestamp
            ))
            self.ui.events_table.setItem(row, 0, timestamp_cell)
            value_cell = QTableWidgetItem(str(
                event.val
            ))

            if len(error_list[row]) > 0:
                value_cell.setBackground(QColor(255, 200, 200))

            self.ui.events_table.setItem(row, 1, value_cell)
            row += 1
        self.ui.events_table.resizeColumnsToContents()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING   )
    app = QApplication(sys.argv)
    _ = PbFileBrowser()
    sys.exit(app.exec_())