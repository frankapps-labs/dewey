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
        ("start_method", "forkserver"),
        ("failure_details", "all"),
    ],
)
def test_invalid_settings_fail_before_start(field, value):
    with pytest.raises(ValueError):
        config(**{field: value})


@pytest.mark.skipif(os.name != "posix", reason="Public supervisor is a POSIX service boundary")
@pytest.mark.parametrize(
    "mode,action,start_method",
    [
        ("sync", "stop", "spawn"),
        ("async", "stop", "spawn"),
        ("sync", "parent-death", "spawn"),
        ("stall", "watchdog", "spawn"),
        ("bootstrap-stall", "watchdog", "spawn"),
        ("cleanup-error", "stop", "spawn"),
        ("sync", "stop", "fork"),
        ("async", "stop", "fork"),
        ("sync", "parent-death", "fork"),
        ("stall", "watchdog", "fork"),
        ("bootstrap-stall", "watchdog", "fork"),
    ],
)
def test_actual_public_cli_contains_and_disposes(tmp_path, mode, action, start_method):
    events = tmp_path / "events"
    events.mkdir()
    settings = tmp_path / "worker.json"
    settings.write_text(
        json.dumps(
            config(
                startup_seconds=0.7 if mode == "bootstrap-stall" else 10,
                supervision_seconds=0.7,
                start_method=start_method,
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
        assert all(record["pgid"] == parent["pid"] for record in records)
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


@pytest.mark.skipif(os.name != "posix", reason="Public supervisor is a POSIX service boundary")
def test_startup_is_paced_by_progress_not_one_deadline(tmp_path):
    """Importing the application and starting each task process each have
    startup_seconds; together they may take longer, as they do on a small CPU."""
    events = tmp_path / "events"
    events.mkdir()
    settings = tmp_path / "worker.json"
    # Readiness and the schedule check take a second each, then two task processes
    # take a second each: about four seconds of startup against a two-second budget.
    settings.write_text(json.dumps(config(startup_seconds=2, concurrency=2).to_dict()))
    root = Path(__file__).resolve().parents[2]
    env = {
        **os.environ,
        "PYTHONPATH": str(root / "src") + os.pathsep + str(root),
        "DEWEY_PUBLIC_TEST_EVENTS": str(events),
        "DEWEY_PUBLIC_TEST_MODE": "slow-start",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "dewey.worker", "--config", str(settings)],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=(tmp_path / "stderr").open("w"),
    )
    try:
        until = time.monotonic() + 15
        while time.monotonic() < until and proc.poll() is None:
            if len(list(events.glob("*-task-*.ready"))) == 2:
                break
            time.sleep(0.02)
        assert proc.poll() is None, (
            f"worker exited with {proc.returncode} during startup: "
            + (tmp_path / "stderr").read_text()
        )
        assert len(list(events.glob("*-task-*.ready"))) == 2
        os.kill(proc.pid, signal.SIGTERM)
        assert proc.wait(timeout=8) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


@pytest.mark.skipif(os.name != "posix", reason="fork is a POSIX start method")
def test_a_stop_signal_in_the_forked_services_window_reaches_its_own_handler(monkeypatch):
    """Between the fork and service() installing its handler, the child holds the
    supervisor's handler, whose closure is a dead copy there. The signals are
    blocked across that window and delivered once the service's handler exists."""
    from dewey.worker import _process

    def supervisor_stop(signum, frame):  # the supervisor's handler, as supervise() installs it
        pass

    stop_signals = (signal.SIGTERM, signal.SIGINT)
    previous = {sig: signal.signal(sig, supervisor_stop) for sig in stop_signals}

    def service_in_the_child() -> int:
        if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
            return 3  # still the supervisor's handler
        if signal.SIGTERM not in signal.pthread_sigmask(signal.SIG_BLOCK, set()):
            return 4  # a stop now would be lost
        os.kill(os.getpid(), signal.SIGTERM)  # arrives in the window
        stopped = []
        signal.signal(signal.SIGTERM, lambda *args: stopped.append(True))  # as service() does
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
        time.sleep(0.05)
        return 0 if stopped else 5  # delivered to the handler installed after the window

    monkeypatch.setattr(_process, "service", service_in_the_child)
    rfd, wfd = os.pipe()
    try:
        child = _process.fork_service({}, wfd, rfd)
        assert child.wait(5) == 0
        assert not signal.pthread_sigmask(signal.SIG_BLOCK, set()) & set(stop_signals)
    finally:
        os.close(rfd)
        os.close(wfd)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


@pytest.mark.skipif(os.name != "posix", reason="fork is a POSIX start method")
def test_a_failed_service_fork_gives_the_caller_its_signal_mask_back(monkeypatch):
    """os.fork can fail (no memory, a process limit): the stop signals blocked
    for the fork must not stay blocked in the caller, nor the handshake pipe open."""
    from dewey.worker import _process

    def no_fork() -> int:
        raise OSError(11, "Resource temporarily unavailable")

    real_pipe = os.pipe
    handshakes: list[tuple[int, int]] = []

    def recorded_pipe() -> tuple[int, int]:
        fds = real_pipe()
        handshakes.append(fds)
        return fds

    rfd, wfd = os.pipe()
    monkeypatch.setattr(_process.os, "fork", no_fork)
    monkeypatch.setattr(_process.os, "pipe", recorded_pipe)
    before = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    try:
        with pytest.raises(OSError):
            _process.fork_service({}, wfd, rfd)
        assert signal.pthread_sigmask(signal.SIG_BLOCK, set()) == before
        assert len(handshakes) == 1
        for fd in handshakes[0]:
            with pytest.raises(OSError):
                os.fstat(fd)  # closed
    finally:
        os.close(rfd)
        os.close(wfd)


@pytest.mark.skipif(os.name != "posix", reason="fork is a POSIX start method")
def test_the_forked_service_flushes_the_hosts_logging_before_it_exits(monkeypatch, tmp_path):
    """os._exit runs no atexit hooks: a buffered or queued host handler would lose
    the lines that explain an exit 70. The child shuts logging down first."""
    import logging

    from dewey.worker import _process

    flushed = tmp_path / "flushed"

    class Buffered(logging.Handler):
        def __init__(self):
            super().__init__()
            self.records = []

        def emit(self, record):
            self.records.append(record.getMessage())

        def flush(self):
            if self.records:
                flushed.write_text("\\n".join(self.records))

    handler = Buffered()
    logging.getLogger("dewey.worker").addHandler(handler)

    def service_in_the_child() -> int:
        logging.getLogger("dewey.worker").error("Native service failed (synthetic)")
        return 70

    monkeypatch.setattr(_process, "service", service_in_the_child)
    rfd, wfd = os.pipe()
    try:
        child = _process.fork_service({}, wfd, rfd)
        assert child.wait(5) == 70
        assert flushed.read_text() == "Native service failed (synthetic)"
    finally:
        os.close(rfd)
        os.close(wfd)
        logging.getLogger("dewey.worker").removeHandler(handler)


def test_a_pool_startup_timeout_names_the_worker_setting_in_the_service_log():
    from dewey._worker.pool import PoolBroken
    from dewey.worker import _process

    timed_out = PoolBroken(
        "Worker initialization timed out: a task process took longer than "
        "startup_timeout_seconds=30 to import the application"
    )
    assert "startup_seconds" in _process.startup_hint(timed_out)
    assert _process.startup_hint(PoolBroken("Worker bootstrap failed")) == ""


def test_a_task_process_flushes_spans_within_the_termination_budget(monkeypatch):
    """The flush at close is bounded by what the worker gives a stopping task process."""
    from types import SimpleNamespace

    from dewey.worker import _process

    cfg = config()  # termination_seconds=1 in these tests
    monkeypatch.setattr(_process, "context", lambda role: SimpleNamespace(config=cfg))
    storage = SimpleNamespace(call=lambda operation, *args: None)
    monkeypatch.setattr(
        _process,
        "application",
        lambda ctx: SimpleNamespace(storage=storage, close_resources=lambda: None),
    )
    runtime = _process.create_runtime()
    try:
        assert runtime.flush_seconds == pytest.approx(cfg.timing.termination_seconds * 0.8)
    finally:
        runtime.close()


def test_a_tracing_configuration_error_names_its_fix_in_the_startup_log(monkeypatch, caplog):
    """A worker told to trace without opentelemetry-api must say so, not just
    'Native service failed (RuntimeError)'."""
    import logging
    import sys
    from types import SimpleNamespace

    from dewey.worker import _process

    for name in list(sys.modules):
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            monkeypatch.setitem(sys.modules, name, None)
    cfg = config(tracing=True)
    rfd, wfd = os.pipe()
    monkeypatch.setenv(_process.STATUS_FD, str(wfd))
    monkeypatch.setattr(_process, "context", lambda role: SimpleNamespace(config=cfg))
    monkeypatch.setattr(signal, "signal", lambda sig, handler: None)
    try:
        with caplog.at_level(logging.ERROR, logger="dewey.worker"):
            assert _process.service() == 70
        assert "dewey[otel]" in caplog.text
    finally:
        os.close(rfd)
