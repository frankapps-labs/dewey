"""Synthetic child runtime proving async handlers behind the sync pool protocol."""

import asyncio
import os
from pathlib import Path

from dewey._worker.loop import ChildLoop
from dewey._worker.pool import Assignment


class AsyncRuntime:
    def __init__(self):
        self.bridge = ChildLoop()
        self.directory = Path(os.environ["DEWEY_WORKER_TEST_DIRECTORY"])

    async def handler(self, assignment):
        if assignment.task_id == "hang":
            await asyncio.sleep(60)
        await asyncio.sleep(0)
        (self.directory / assignment.task_id).write_text(str(os.getpid()))

    def execute(self, assignment: Assignment):
        self.bridge.invoke(self.handler, assignment)

    def close(self):
        self.bridge.close()
        (self.directory / "loop-closed").write_text(str(os.getpid()))


def create_runtime():
    return AsyncRuntime()
