import random

import pytest

from folder_sync.folder_sync import _run_executer_with_progress


class TestRunExecuter:
    def test_normal(self):
        data = list(range(100_000))
        assert _run_executer_with_progress(lambda v: v, [(d,) for d in data], 5) == data

    def test_order(self):
        data = list(range(100_000))
        assert (
            _run_executer_with_progress(
                lambda v: v,
                [(d,) for d in data],
                5,
                order=[random.randint(0, 100) for _ in range(100_000)],
            )
            == data
        )

    def test_datapoints_per_future(self):
        data = list(range(100_000))
        assert (
            _run_executer_with_progress(
                lambda v: v, [(d,) for d in data], 5, datapoints_per_future=1000
            )
            == data
        )

    def test_datapoints_per_future_order(self):
        data = list(range(100_000))
        assert (
            _run_executer_with_progress(
                lambda v: v,
                [(d,) for d in data],
                5,
                order=[random.randint(0, 100) for _ in range(100_000)],
                datapoints_per_future=1000,
            )
            == data
        )
