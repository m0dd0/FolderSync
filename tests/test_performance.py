import time

import pytest

from folder_sync import folder_sync as fs

from .fixtures import empty_folders, create_empty_folders, create_random_files


class TestFixturePerformance:
    @pytest.mark.parametrize(
        "branching, depth",
        [(10, 3), (10, 4)],
    )
    def test_empty_folder_creation(self, branching, depth, benchmark, tmp_path):
        benchmark.pedantic(
            create_empty_folders, (tmp_path, branching, depth), rounds=1, iterations=1
        )

    @pytest.mark.parametrize(
        "component_weights, means, vars, n_files",
        [
            ((0.5, 0.5), (5e6, 5e3), (1e6, 1e3), 1000),
        ],
    )
    def test_file_creation(
        self, component_weights, means, vars, n_files, benchmark, tmp_path
    ):
        benchmark.pedantic(
            create_random_files,
            (component_weights, means, vars, n_files, tmp_path),
            rounds=1,
            iterations=1,
        )

        assert len(list(tmp_path.rglob("*"))) == n_files


@pytest.mark.parametrize(
    "empty_folders, n_threads, datapoints_per_future",
    [
        # ((10, 3), 4, 1),
        # ((10, 3), 1, 1),
        # ((10, 4), 1, 1),
        # ((10, 4), 4, 1),
        ((10, 4), 10, 1),
        # ((10, 4), 100, 1),
        # ((10, 4), 1000, 1),
        # ((10, 4), 10000, 1),
        ((10, 4), 10, 10),
        ((10, 4), 10, 100),  # this seems to be the sweet spot
        ((10, 4), 10, 1000),
    ],
    indirect=["empty_folders"],
)
def test_benchmark_parallel_folder_deletion(
    benchmark, empty_folders, n_threads, datapoints_per_future
):
    # folder creation takes some time so do not wonder if this takes longer than expected
    data = [(p,) for p in empty_folders.rglob("*")]
    order = [-len(p[0].parts) for p in data]
    benchmark.pedantic(
        fs._run_executer_with_progress,
        (
            n_threads,
            lambda p: p.rmdir(),
            data,
            order,
            datapoints_per_future,
        ),
        rounds=1,
        iterations=1,
    )

    assert len(list(empty_folders.rglob("*"))) == 0


@pytest.mark.parametrize(
    "empty_folders",
    [
        # (10, 3),
        (10, 4),
        # (10, 5),
    ],
    indirect=["empty_folders"],
)
def test_benchmark_sequential_folder_deletion(benchmark, empty_folders):
    # folder creation takes some time so do not wonder if this takes longer than expected
    data = [p for p in empty_folders.rglob("*")]
    order = [-len(p.parts) for p in data]
    sorted_data = [d for _, d in sorted(zip(order, data))]

    benchmark.pedantic(
        lambda ps: [p.rmdir() for p in ps], (sorted_data,), rounds=1, iterations=1
    )

    assert len(list(empty_folders.rglob("*"))) == 0
