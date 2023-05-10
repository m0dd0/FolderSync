import pytest
import logging

# from .test_data import fixtures
from .fixtures import source_folders, target_folder

# import seedir

from folder_sync import sync_folders


def assert_subset_folder(superset_folder, subset_folder):
    for elem in superset_folder.iterdir():
        assert (subset_folder / elem.name).exists()
        if elem.is_dir():
            assert (subset_folder / elem.name).is_dir()
            assert_subset_folder(elem, subset_folder / elem.name)
        elif elem.is_file():
            assert (subset_folder / elem.name).is_file()
            assert (subset_folder / elem.name).read_bytes() == elem.read_bytes()
            assert (subset_folder / elem.name).stat().st_mtime == elem.stat().st_mtime
            assert (subset_folder / elem.name).stat().st_size == elem.stat().st_size


@pytest.mark.parametrize(
    "source_folders, target_folder",
    fixtures.ALL_FOLDER_COMBINATIONS,
    # [
    # ("basic", "empty"),
    # ("basic", "trimmed"),
    # ("basic", "basic"),
    # ("basic", "less_empty_folders"),
    # ("basic", "more_empty_folders"),
    # ("basic", "renamed_file2folder"),
    # ("basic", "changed_data"),
    # ],
    indirect=["source_folders", "target_folder"],
)
def test_sync(source_folders, target_folder):
    source_folder, control_source_folder = source_folders

    logging.basicConfig(level=logging.INFO)
    sync_folders(source_folder, target_folder, ask=False)

    # NOTE: we do not combine the subfolder checks to one assert because it makes it easier to see which check fails

    # are all files and folders in source also in target?
    assert_subset_folder(source_folder, target_folder)
    # are all files and folders in target also in source?
    # (if there are elements in target which are not in source, they should have been deleted)
    assert_subset_folder(target_folder, source_folder)

    # has source folder not been changed?
    assert_subset_folder(control_source_folder, source_folder)
    assert_subset_folder(source_folder, control_source_folder)

    # print("Control Source")
    # seedir.seedir(control_source_folder, style="emoji")
    # print("Source")
    # seedir.seedir(source_folder, style="emoji")
    # print("Target")
    # seedir.seedir(target_folder, style="emoji")
    # assert False
