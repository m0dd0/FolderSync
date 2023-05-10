from pathlib import Path
import shutil

import pytest

TEST_DATA = Path(__file__).parent / "test_data"
TEST_FOLDERS = [p.name for p in TEST_DATA.iterdir() if p.is_dir()]
ALL_FOLDER_COMBINATIONS = [
    (source_folder, target_folder)
    for source_folder in TEST_FOLDERS
    for target_folder in TEST_FOLDERS
]


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
