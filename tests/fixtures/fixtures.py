from pathlib import Path
import shutil
from typing import Tuple, List

import numpy as np
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


def create_random_files(component_weights, means, vars, n_files, root: Path):
    for i in range(n_files):
        component_idx = np.random.choice(len(component_weights), p=component_weights)
        size = int(np.random.normal(means[component_idx], vars[component_idx]))
        with open(root / str(i), "wb") as f:
            f.write(np.random.bytes(size))


@pytest.fixture
def random_files(request: pytest.FixtureRequest, tmp_path: Path) -> List[Path]:
    component_weights, means, vars = zip(*request.param[0])
    n_files = request.param[1]
    create_random_files(component_weights, means, vars, n_files, tmp_path)
    return tmp_path
