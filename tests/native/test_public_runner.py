"""Transport consumers need only public runner types, including across pickle/spawn."""

import os
import pickle
import subprocess
import sys
from pathlib import Path

import pytest

from dewey.runner import RunnerAssignment, RunnerTiming


@pytest.mark.parametrize("kind", [RunnerAssignment, RunnerTiming])
def test_runner_value_types_are_defined_on_the_public_surface(kind):
    assert kind.__module__ == "dewey.runner"
    assert kind.__name__.startswith("Runner")
    value = (
        kind("synthetic-task", "synthetic-dispatch")
        if kind is RunnerAssignment
        else kind(30, 30, 2, 1, 0.1, 1, 60)
    )
    assert pickle.loads(pickle.dumps(value)) == value


def test_importing_public_runner_does_not_load_private_worker_runtime():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from dewey.runner import FailureReport, LeaseExpired, RunnerAssignment, RunnerPolicy, RunnerTiming, PROTOCOL_VERSION; import sys; assert not any(name.startswith('dewey._worker') for name in sys.modules); print(PROTOCOL_VERSION)",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / "src")),
    )
    assert result.stdout.strip() == "1"


def test_internal_worker_uses_the_same_public_value_types():
    from dewey._worker.contracts import Timing
    from dewey._worker.pool import Assignment

    assert Timing is RunnerTiming
    assert Assignment is RunnerAssignment
