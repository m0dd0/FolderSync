import time
import shutil
from pathlib import Path

import pytest

from folder_sync import folder_sync as fs

from .fixtures import (
    empty_folders,
    create_empty_folders,
    create_random_files,
    random_files,
)


class TestFixturePerformance:
    @pytest.mark.parametrize(
        "branching, depth",
        [(10, 3), (10, 4)],
    )
    def test_benchmark_fixture_folder_creation(
        self, branching, depth, benchmark, tmp_path
    ):
        benchmark.pedantic(
            create_empty_folders, (tmp_path, branching, depth), rounds=1, iterations=1
        )

    @pytest.mark.parametrize(
        "file_distribution",
        [
            {5e6: 500, 5e3: 500},
        ],
    )
    def test_benchmark_fixture_file_creation(
        self, file_distribution, benchmark, tmp_path
    ):
        benchmark.pedantic(
            create_random_files,
            (file_distribution, tmp_path),
            rounds=1,
            iterations=1,
        )

        assert len(list(tmp_path.rglob("*"))) == sum(file_distribution.values())


class TestFileOperationPerformance:
    @pytest.mark.parametrize(
        "empty_folders, n_threads, datapoints_per_future",
        [
            ((10, 4), 10, 1),
            ((10, 4), 10, 10),
            ((10, 4), 10, 100),  # this seems to be the sweet spot
            ((10, 4), 100, 1),
            ((10, 4), 100, 10),
            ((10, 4), 100, 100),
        ],
        indirect=["empty_folders"],
    )
    def test_benchmark_folder_deletion(
        self, empty_folders: Path, n_threads: int, datapoints_per_future: int, benchmark
    ):
        # folder creation takes some time so do not wonder if this takes longer than expected
        paths = [(p,) for p in empty_folders.rglob("*")]
        order = [-len(p[0].parts) for p in paths]
        benchmark.pedantic(
            fs._run_executer_with_progress,
            (
                lambda p: p.rmdir(),
                paths,
                n_threads,
                order,
                datapoints_per_future,
            ),
            rounds=1,
            iterations=1,
        )

        assert len(list(empty_folders.rglob("*"))) == 0

    @pytest.mark.parametrize(
        "random_files, n_threads, datapoints_per_future",
        [
            ({5e6: 500, 5e3: 500}, 10, 1),
            ({5e6: 500, 5e3: 500}, 10, 10),
            ({5e6: 500, 5e3: 500}, 10, 100),
            ({5e6: 500, 5e3: 500}, 100, 1),
            ({5e6: 500, 5e3: 500}, 100, 10),
            ({5e6: 500, 5e3: 500}, 100, 100),
        ],
        indirect=["random_files"],
    )
    def test_benchmark_file_deletion(
        self, random_files: Path, n_threads: int, datapoints_per_future: int, benchmark
    ):
        paths = [(p,) for p in random_files.rglob("*")]
        benchmark.pedantic(
            fs._run_executer_with_progress,
            (lambda p: p.unlink(), paths, n_threads),
            {"datapoints_per_future": datapoints_per_future},
            rounds=1,
            iterations=1,
        )

    @pytest.mark.parametrize(
        "n_folders, n_threads, datapoints_per_future",
        [
            (10_000, 10, 1),
            (10_000, 10, 10),
            (10_000, 10, 100),
            (10_000, 100, 1),
            (10_000, 100, 10),
            (10_000, 100, 100),
        ],
    )
    def test_benchmark_folder_creation(
        self,
        n_folders: int,
        n_threads: int,
        datapoints_per_future: int,
        tmp_path: Path,
        benchmark,
    ):
        benchmark.pedantic(
            fs._run_executer_with_progress,
            (
                lambda p: p.mkdir(),
                [(tmp_path / str(i),) for i in range(n_folders)],
                n_threads,
            ),
            {"datapoints_per_future": datapoints_per_future},
            rounds=1,
            iterations=1,
        )

    @pytest.mark.parametrize(
        "random_files, n_threads, datapoints_per_future",
        [
            ({5e6: 500, 5e3: 500}, 10, 1),
            ({5e6: 500, 5e3: 500}, 10, 10),
            ({5e6: 500, 5e3: 500}, 10, 100),
            ({5e6: 500, 5e3: 500}, 100, 1),
            ({5e6: 500, 5e3: 500}, 100, 10),
            ({5e6: 500, 5e3: 500}, 100, 100),
        ],
        indirect=["random_files"],
    )
    def test_benchmark_file_copy(
        self, random_files: Path, n_threads: int, datapoints_per_future: int, benchmark
    ):
        source_paths = [(p,) for p in random_files.rglob("*")]
        target_path = random_files / "copied"
        target_path.mkdir()

        benchmark.pedantic(
            fs._run_executer_with_progress,
            (
                lambda p: shutil.copy2(p, target_path),
                source_paths,
                n_threads,
            ),
            {"datapoints_per_future": datapoints_per_future},
            rounds=1,
            iterations=1,
        )


# def test_benchmark_executer_overhead(benchmark):
#     repetitions = 1000
#     execution_time = 0.001

#     benchmark(
#         # lambda: [time.sleep(execution_time) for _ in range(repetitions)],
#         #####
#         fs._run_executer_with_progress,
#         lambda: [time.sleep(execution_time) for _ in range(repetitions)],
#         [[] * repetitions],
#         1,
#         datapoints_per_future=2 * repetitions,
#     )

#     # --> there is effectively no overhead
#     # --> we do not need a sequential testcase as baseline as the executer will do the same thing
