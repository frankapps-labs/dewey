"""Storage-free synthetic applications for the public supervised CLI controls."""

import asyncio
import json
import os
import time
from pathlib import Path

from dewey.worker import WorkerApplication


def create_application(context):
    root = Path(os.environ["DEWEY_PUBLIC_TEST_EVENTS"])
    name = f"{context.owner}-{context.role}-{os.getpid()}"
    (root / (name + ".ready")).write_text(
        json.dumps(
            {"pid": os.getpid(), "pgid": os.getpgrp(), "role": context.role, "owner": context.owner}
        )
    )
    mode = os.environ.get("DEWEY_PUBLIC_TEST_MODE", "sync")
    if mode == "bootstrap-stall" and context.role == "parent":
        time.sleep(20)
    if mode == "slow-start" and context.role == "task":
        time.sleep(0.8)  # each task process takes a while to come up, but under budget

    class Storage:
        loop = None

        def operation(self, operation):
            if mode == "stall" and context.role == "parent" and operation == "recover":
                time.sleep(20)
            if (
                mode == "slow-start"
                and context.role == "parent"
                and operation in ("readiness", "ops")
            ):
                time.sleep(1.0)  # bounded by db_wait_seconds in a real storage
            return [] if operation in ("claim", "recover") else None

        async def awaited(self, operation):
            loop = asyncio.get_running_loop()
            if self.loop is None:
                self.loop = loop
            assert self.loop is loop
            return self.operation(operation)

        def call(self, operation, *args):
            return self.awaited(operation) if mode == "async" else self.operation(operation)

    storage = Storage()

    def record_close():
        if mode == "cleanup-error":
            raise ValueError("SYNTHETIC_SENSITIVE_ERROR_DETAILS")
        (root / (name + ".closed")).touch()

    async def close_async():
        assert asyncio.get_running_loop() is storage.loop
        record_close()

    return WorkerApplication(storage, close_async if mode == "async" else record_close)
