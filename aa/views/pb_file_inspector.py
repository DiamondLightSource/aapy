import sys
import os
from PyQt5.QtWidgets import QMainWindow, QApplication, QTableWidgetItem
from PyQt5 import uic
from PyQt5.QtGui import QColor, QPalette
from PyQt5.QtCore import QSize
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
        form.input_file_path.editingFinished.connect(self.update_save_dir)
        form.same_dir_as_input.stateChanged.connect(self.update_save_dir)
        form.save_button.clicked.connect(self.save_pb_file)
        form.delete_event_button.clicked.connect(self.delete_selected_events)
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
                self.set_status(f"No such file: {input_path}", is_bad=True)
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

    def set_status(self, status_string, is_bad=False):
        self.ui.status_box.setText(status_string)
        palette = QPalette()
        # Red message text for an error
        palette.setColor(QPalette.Text, QColor(120 if is_bad else 0, 0, 0))
        self.ui.status_box.setPalette(palette)

    def update_save_dir(self):
        if self.ui.same_dir_as_input.isChecked():
            self.ui.save_dir.setText(
                os.path.dirname(
                    self.ui.input_file_path.text()
                )
            )

    def decode_events_and_check(self):
        if self.pb_file.payload_info is None:
            self.set_status(f"Need to load a file first", is_bad=True)
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
        self.error_list = [[]] * rows
        self.populate_errors_list()

        self.populate_events_table()
        error_count = len(self.pb_file.decoding_errors)
        event_count = len(self.pb_file.pb_events)

        self.set_status(f"Decoded {event_count} events "
                        f"with {error_count} errors")

    def populate_errors_list(self):
        """Display the list of parsing errors in the Errors widget"""
        self.ui.errors_list.clear()
        for index, error in self.pb_file.decoding_errors:
            self.error_list[index].append(error)
            error_string = pb_validation.PB_ERROR_STRINGS[error]
            self.ui.errors_list.addItem(
                f"{error_string} at {index}"
            )
        if len(self.pb_file.decoding_errors) <= 1:
            self.ui.errors_list.addItem("No errors found")

    def populate_events_table(self):
        """Populate the events table and errors widget from stored info"""
        row = 0
        for event in self.pb_file.pb_events:
            legible_timestamp = get_iso_timestamp_for_event(
                self.pb_file.payload_info.year,
                event
            )

            timestamp_cell = QTableWidgetItem(str(
                legible_timestamp
            ))
            self.ui.events_table.setItem(row, 0, timestamp_cell)
            value_cell = QTableWidgetItem(str(
                event.val
            ))

            if len(self.error_list[row]) > 0:
                value_cell.setBackground(QColor(255, 200, 200))

            self.ui.events_table.setItem(row, 1, value_cell)

            extra_fields_cell = QTableWidgetItem(
                repr(event.fieldvalues)
            )
            extra_fields_cell.setSizeHint(QSize(500, 10))
            self.ui.events_table.setItem(row, 2, extra_fields_cell)

            if len(self.error_list[row]) > 0:
                error_string = pb_validation.PB_ERROR_STRINGS[
                    self.error_list[row][0]]
                if len(self.error_list[row]) > 1:
                    error_string += " ..."
                error_cell = QTableWidgetItem(
                    error_string
                )
                self.ui.events_table.setItem(row, 3, error_cell)

            row += 1
        self.ui.events_table.resizeColumnsToContents()

    def delete_selected_events(self):
        # Build a list of indices to delete
        # Get the row indices from selection
        selected_indices = self.ui.events_table.selectedIndexes()
        selected_rows = []
        for q_model_index in selected_indices:
            if not q_model_index.row() in selected_rows:
                selected_rows.append(q_model_index.row())

        # We take descending order so we can delete several events in one go
        selected_rows = sorted(selected_rows, reverse=True)

        for event_index in selected_rows:
            self.pb_file.raw_lines.pop(event_index)
        self.decode_events_and_check()
        self.set_status(f"Deleted events: {str(selected_rows)}")

    def save_pb_file(self):
        try:
            self.pb_file.serialize_to_raw_lines()
        except Exception as e:
            self.set_status(f"Exception encoding events: {e}", is_bad=True)
            return False

        save_dir = self.ui.save_dir.text().strip()
        if not os.path.isdir(save_dir):
            self.set_status(f"Save directory does not exist: {save_dir}",
                            is_bad=True)
            return False

        orig_filename = self.ui.input_file_path.text()
        orig_stem = orig_filename.split(".")[0]
        new_filename = orig_stem + self.ui.save_suffix.text().strip() + ".pb"
        save_path = os.path.join(
            save_dir,
            new_filename
        )
        if os.path.exists(save_path):
            self.set_status(f"Won't save: A file already "
                            f"exists here: {save_path}", is_bad=True)
            return False
        try:
            self.pb_file.write_raw_lines_to_file(save_path)
        except Exception as e:
            self.set_status(f"Exception saving file: {e}, is_bad=True")
            return False
        else:
            self.set_status(f"Saved file to {save_path}")


def get_iso_timestamp_for_event(year, event):
    timestamp = pb.event_timestamp(year, event)
    timezone = pytz.timezone("Europe/London")
    return datetime.datetime.fromtimestamp(
        timestamp,
        timezone
    ).isoformat()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    app = QApplication(sys.argv)
    _ = PbFileBrowser()
    sys.exit(app.exec_())