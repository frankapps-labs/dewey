"""Actual public CLI supervision; no DB/broker needed for lifecycle controls."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from dewey.worker import WorkerConfig, WorkerTiming, run_worker
from dewey.worker._process import group_alive


def config(**overrides):
    values = {
        "factory": "tests._helpers.public_worker_runtime:create_application",
        "queues": ("synthetic",),
        "timing": WorkerTiming(30, 30, 2, 1, 0.1, 1, 60),
        "concurrency": 2,
        "max_restarts": 0,
        "startup_seconds": 10,
        "supervision_seconds": 2,
        "shutdown_seconds": 1,
    }
    values.update(overrides)
    return WorkerConfig(**values)


def test_public_entrypoint_rejects_invalid_or_async_callers_before_spawn():
    with pytest.raises(TypeError):
        run_worker(None)

    async def run():
        with pytest.raises(RuntimeError, match="event loop"):
            run_worker(config())

    asyncio.run(run())


def test_public_settings_roundtrip_and_explicit_shape():
    expected = config()
    assert WorkerConfig.from_dict(expected.to_dict()) == expected
    with pytest.raises(ValueError):
        WorkerConfig.from_dict({})
    extra = expected.to_dict()
    extra["database_password"] = "synthetic-not-a-secret"
    with pytest.raises(ValueError):
        WorkerConfig.from_dict(extra)


@pytest.mark.parametrize(
    "field,value",
    [
        ("queues", "synthetic"),
        ("queues", []),
        ("concurrency", True),
        ("max_restarts", -1),
        ("startup_seconds", 0),
        ("supervision_seconds", float("nan")),
        ("shutdown_seconds", True),
        ("factory", "not-a-factory"),
        ("concurrency", 100),
    ],
)
def test_invalid_settings_fail_before_start(field, value):
    with pytest.raises(ValueError):
        config(**{field: value})


@pytest.mark.skipif(os.name != "posix", reason="Public supervisor is a POSIX service boundary")
@pytest.mark.parametrize(
    "mode,action",
    [
        ("sync", "stop"),
        ("async", "stop"),
        ("sync", "parent-death"),
        ("stall", "watchdog"),
        ("bootstrap-stall", "watchdog"),
        ("cleanup-error", "stop"),
    ],
)
def test_actual_public_cli_contains_and_disposes(tmp_path, mode, action):
    events = tmp_path / "events"
    events.mkdir()
    settings = tmp_path / "worker.json"
    settings.write_text(
        json.dumps(
            config(
                startup_seconds=0.7 if mode == "bootstrap-stall" else 10, supervision_seconds=0.7
            ).to_dict()
        )
    )
    root = Path(__file__).resolve().parents[2]
    env = {
        **os.environ,
        "PYTHONPATH": str(root / "src") + os.pathsep + str(root),
        "DEWEY_PUBLIC_TEST_EVENTS": str(events),
        "DEWEY_PUBLIC_TEST_MODE": mode,
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    stderr = (tmp_path / "stderr").open("w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "dewey.worker", "--config", str(settings)],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=stderr,
    )
    records = []
    try:
        until = time.monotonic() + 12
        while time.monotonic() < until:
            records = [json.loads(path.read_text()) for path in events.glob("*.ready")]
            if (mode == "bootstrap-stall" and records) or (len(records) == 3):
                break
            time.sleep(0.02)
        else:
            pytest.fail("Public worker did not initialize its process-local application")
        parent = next(record for record in records if record["role"] == "parent")
        assert parent["pid"] != proc.pid
        assert parent["pgid"] == parent["pid"]
        if action == "stop":
            os.kill(proc.pid, signal.SIGTERM)
        elif action == "parent-death":
            os.kill(parent["pid"], signal.SIGKILL)  # Never manually kill its group.
        proc.wait(timeout=8)
        assert proc.returncode == (0 if action == "stop" and mode != "cleanup-error" else 70)
        assert not group_alive(parent["pgid"])
        if action == "stop" and mode != "cleanup-error":
            assert len(list(events.glob("*.closed"))) == 3
        stderr.flush()
        assert "SYNTHETIC_SENSITIVE_ERROR_DETAILS" not in (tmp_path / "stderr").read_text()
    finally:
        # Rescue is test failure, never proof of automatic containment.
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        for record in records:
            if record["role"] == "parent" and group_alive(record["pgid"]):
                os.killpg(record["pgid"], signal.SIGKILL)
        stderr.close()
