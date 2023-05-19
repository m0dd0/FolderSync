import logging
from pathlib import Path

# import seedir
import pytest

from folder_sync import sync_folders

from . import fixtures
from .fixtures import source_folders, target_folder


def assert_identical_folder(folder_a: Path, folder_b: Path):
    paths_a = folder_a.rglob("*")
    paths_b = folder_b.rglob("*")

    paths_a_rel = set([p.relative_to(folder_a) for p in paths_a])
    paths_b_rel = set([p.relative_to(folder_b) for p in paths_b])

    assert paths_a_rel == paths_b_rel

    for path_a_rel in paths_a_rel:
        path_a = folder_a / path_a_rel
        path_b = folder_b / path_a_rel

        if path_a.is_file():
            assert path_b.is_file()
            assert (
                path_a.stat().st_mtime == path_b.stat().st_mtime
            ), f"{path_a} {path_b}"
            assert path_a.stat().st_size == path_b.stat().st_size
            assert path_a.read_bytes() == path_b.read_bytes()

        elif path_a.is_dir():
            assert path_b.is_dir()
        else:
            raise ValueError(f"Unknown path type: {path_a}")


@pytest.mark.parametrize(
    "source_folders, target_folder",
    fixtures.ALL_FOLDER_COMBINATIONS,
    # [("basic", "changed_data")],
    indirect=["source_folders", "target_folder"],
)
def test_sync(source_folders, target_folder, caplog):
    source_folder, control_source_folder = source_folders

    logging.basicConfig(level=logging.INFO)
    caplog.set_level(logging.INFO)
    sync_folders(source_folder, target_folder, quiet=True)

    # print("Control Source")
    # seedir.seedir(control_source_folder, style="emoji")
    # print("Source")
    # seedir.seedir(source_folder, style="emoji")
    # print("Target")
    # seedir.seedir(target_folder, style="emoji")
    # assert False
