import pytest
from pathlib import Path
import shutil

import seedir

from folder_sync import sync_folders


TEST_DATA = Path(__file__).parent / "test_data"


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


def setup_folder(name, root):
    shutil.copytree(TEST_DATA / name, root / name)
    for path in (root / name).rglob(".gitkeep"):
        path.unlink()

    return root / name


@pytest.fixture
def source_folders(request, tmp_path_factory):
    return (
        setup_folder(request.param, tmp_path_factory.mktemp("temp_folder")),
        setup_folder(request.param, tmp_path_factory.mktemp("temp_folder")),
    )


@pytest.fixture
def target_folder(request, tmp_path):
    return setup_folder(request.param, tmp_path)


@pytest.mark.parametrize(
    "source_folders, target_folder",
    [
        ("basic", "empty"),
        ("empty", "basic"),
        ("basic", "basic"),
        ("empty", "empty"),
        ("basic", "trimmed"),
        ("trimmed", "basic"),
        ("basic", "more_empty_folders"),
        ("more_empty_folders", "basic"),
        ("less_empty_folders", "basic"),
        ("basic", "less_empty_folders"),
    ],
    indirect=["source_folders", "target_folder"],
)
def test_sync(source_folders, target_folder):
    source_folder, control_source_folder = source_folders

    sync_folders(source_folder, target_folder)

    # NOTE: we do not combine the subfolder checks to one assert because it makes it easier to see which check fails

    # are all files and folders in source also in target?
    assert_subset_folder(source_folder, target_folder)
    # are all files and folders in target also in source?
    # (if there are elements in target which are not in source, they should have been deleted)
    assert_subset_folder(target_folder, source_folder)

    # has source folder not been changed?
    assert_subset_folder(control_source_folder, source_folder)
    assert_subset_folder(source_folder, control_source_folder)

    print("Control Source")
    seedir.seedir(control_source_folder, style="emoji")
    print("Source")
    seedir.seedir(source_folder, style="emoji")
    print("Target")
    seedir.seedir(target_folder, style="emoji")
    # assert False
