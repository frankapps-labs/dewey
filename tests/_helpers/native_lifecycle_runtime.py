"""Synthetic barriers around the real native ledger runtime for lifecycle tests."""

import os
import time
from pathlib import Path

import dewey
from tests._helpers.native_runtime import create_runtime as candidate_factory


@dewey.task("native.synthetic.barrier")
def barrier(release, effect):
    while not Path(release).exists():
        time.sleep(0.005)
    Path(effect).touch()


class DelayedReturn:
    def __init__(self):
        self.inner = candidate_factory()

    def execute(self, assignment):
        self.inner.execute(assignment)
        if os.environ.get("DEWEY_NATIVE_TEST_HOLD_RETURN") == "1":
            time.sleep(60)

    def close(self):
        self.inner.close()


def create_runtime():
    return DelayedReturn()
