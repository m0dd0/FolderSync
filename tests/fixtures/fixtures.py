from pathlib import Path
import shutil
from typing import Tuple, List, Dict
import os

import pytest

TEST_DATA = Path(__file__).parent
TEST_FOLDERS = [p.name for p in TEST_DATA.iterdir() if p.is_dir()]
ALL_FOLDER_COMBINATIONS = [
    (source_folder, target_folder)
    for source_folder in TEST_FOLDERS
    for target_folder in TEST_FOLDERS
]


def setup_folder(name: str, root: Path) -> Path:
    shutil.copytree(TEST_DATA / name, root / name)
    for path in (root / name).rglob(".gitkeep"):
        path.unlink()

    return root / name


@pytest.fixture
def source_folders(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Tuple[Path, Path]:
    return (
        setup_folder(request.param, tmp_path_factory.mktemp("temp_folder")),
        setup_folder(request.param, tmp_path_factory.mktemp("temp_folder")),
    )


@pytest.fixture
def target_folder(request: pytest.FixtureRequest, tmp_path: Path) -> Path:
    return setup_folder(request.param, tmp_path)


def create_empty_folders(root: Path, branching: int, levels: int):
    if levels == 0:
        return

    for i in range(branching):
        new_folder = root / f"level_{levels}_{i}"
        new_folder.mkdir(exist_ok=True)
        create_empty_folders(new_folder, branching, levels - 1)


@pytest.fixture
def empty_folders(request: pytest.FixtureRequest, tmp_path: Path) -> List[Path]:
    branching, levels = request.param
    create_empty_folders(tmp_path, branching, levels)
    return tmp_path


def create_random_files(file_distribution: Dict[int, int], root: Path):
    for size, n in file_distribution.items():
        n = int(n)
        size = int(size)
        for i in range(n):
            with open(root / f"{size//1e3}kb_{i}", "wb") as f:
                f.write(os.urandom(size))


@pytest.fixture
def random_files(request: pytest.FixtureRequest, tmp_path: Path) -> List[Path]:
    create_random_files(request.param, tmp_path)
    return tmp_path
