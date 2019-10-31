from mock import patch
from aa.pb_tools.views import pb_file_inspector

@patch("aa.pb_tools.views.pb_file_inspector.get_uic_obj")
@patch("aa.pb_tools.views.pb_file_inspector.QMainWindow")
def test_creating_PbFileBrowser(mock_uic, mock_main_window):
    ui = pb_file_inspector.PbFileInspector()

def test_generate_save_path():
    orig_path = "/a/b/c/d.pb"
    save_dir = "/e/f/g"
    save_suffix = "_suffix"
    expected = "/e/f/g/d_suffix.pb"

    assert pb_file_inspector.generate_save_path(
        orig_path,
        save_dir,
        save_suffix
    ) == expected
