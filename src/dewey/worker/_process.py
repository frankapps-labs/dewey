"""POSIX native service and independent supervisor; no framework/broker imports."""

from __future__ import annotations

import asyncio
import ctypes
import importlib
import inspect
import json
import logging
import os
import selectors
import signal
import subprocess
import sys
import threading
import time
from typing import Any
from uuid import uuid4

from dewey._worker.loop import ChildLoop
from dewey._worker.pool import WorkerPool
from dewey._worker.runner import NativeRunner
from dewey._worker.runtime import LedgerRuntime
from dewey.alerts import load_alert_hook
from dewey.policy import registry
from dewey.worker import WorkerApplication, WorkerConfig, WorkerContext

LOG = logging.getLogger("dewey.worker")
OPTIONS = "_DEWEY_WORKER_OPTIONS"
STATUS_FD = "_DEWEY_WORKER_STATUS_FD"
FAULT = 70
CLEANUP_FAILURE = 71


def application(context: WorkerContext) -> WorkerApplication:
    module, name = context.config.factory.split(":")
    factory = getattr(importlib.import_module(module), name)
    if not callable(factory) or inspect.iscoroutinefunction(factory):
        raise TypeError("Worker factory must be a synchronous process-local resource constructor")
    result = factory(context)
    if not isinstance(result, WorkerApplication):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError("Worker factory must return WorkerApplication")
    return result


def context(role: Any) -> WorkerContext:
    value = json.loads(os.environ[OPTIONS])
    return WorkerContext(WorkerConfig.from_dict(value["config"]), value["owner"], role)


def create_runtime() -> LedgerRuntime:
    ctx = context("task")
    load_alert_hook(ctx.config.alert_hook)
    app = application(ctx)
    return LedgerRuntime(app.storage, close_resources=app.close_resources)


def service() -> int:
    ctx = context("parent")
    config = ctx.config
    stopped = False
    forced = False
    deadline = None
    fd = int(os.environ[STATUS_FD])

    def stop(signum: int, frame: Any) -> None:
        nonlocal stopped
        stopped = True

    def pulse(phase: str = "progress") -> None:
        os.write(fd, f"{phase} {time.monotonic()}\n".encode("ascii"))

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    runner = None
    app = None
    code = FAULT
    try:
        load_alert_hook(config.alert_hook)
        app = application(ctx)
        if not registry.task_types():
            LOG.warning("No task handlers are registered; this worker will claim nothing")
        pool = WorkerPool(
            "dewey.worker._process:create_runtime",
            concurrency=config.concurrency,
            task_timeout_seconds=config.timing.task_timeout_seconds,
            startup_timeout_seconds=config.startup_seconds,
            termination_timeout_seconds=config.timing.termination_seconds / 4,
            max_restarts=config.max_restarts,
        )
        runner = NativeRunner(
            pool,
            app.storage,
            timing=config.timing,
            close_resources=app.close_resources,
            progress=pulse,
            can_claim=lambda: not stopped,
            auto_apply_schedules=config.auto_apply_schedules,
        )
        runner.start()
        pulse("ready")
        while True:
            if stopped:
                if deadline is None:
                    deadline = time.monotonic() + config.shutdown_seconds
                if not runner.assignments:
                    break
                if time.monotonic() >= deadline:
                    forced = True
                    break
            runner.step()
            pulse()
            wait_seconds = config.timing.poll_interval_seconds
            if deadline is not None:
                wait_seconds = min(wait_seconds, max(0.0, deadline - time.monotonic()))
            pool.wait(wait_seconds)
        runner.close()
        code = FAULT if forced else 0
    except BaseException as exc:
        LOG.error("Native service failed (%s)", type(exc).__name__)
    finally:
        try:
            if runner is not None:
                runner.close()
            elif app is not None:
                loop = ChildLoop()
                try:
                    loop.invoke(app.close_resources)
                finally:
                    loop.close()
        except BaseException as exc:
            code = FAULT
            LOG.error("Native resource cleanup failed (%s)", type(exc).__name__)
        finally:
            os.close(fd)
    return code


def group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


def reap_adopted(pgid: int) -> None:
    """Call only after Popen reaped its direct child; never steal its exit status."""
    while True:
        try:
            pid, _ = os.waitpid(-pgid, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def subreaper(enable: bool) -> int | None:
    if sys.platform != "linux":
        return None  # macOS launchd reaps the orphan children instead.
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    if libc.prctl(37, ctypes.byref(previous), 0, 0, 0) != 0:
        raise OSError("Cannot inspect Linux child subreaper state")
    if libc.prctl(36, int(enable), 0, 0, 0) != 0:
        raise OSError("Cannot establish Linux child subreaper boundary")
    return previous.value


def contain(child: subprocess.Popen[Any], seconds: float) -> None:
    """Destroy only our exclusive group; no successful exit while members remain."""
    if group_alive(child.pid):
        os.killpg(child.pid, signal.SIGTERM)
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if child.poll() is not None:
            reap_adopted(child.pid)
        if not group_alive(child.pid):
            return
        time.sleep(0.01)
    if group_alive(child.pid):
        os.killpg(child.pid, signal.SIGKILL)
    child.wait(timeout=seconds)
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        reap_adopted(child.pid)
        if not group_alive(child.pid):
            return
        time.sleep(0.01)
    raise TimeoutError("Worker process group could not be physically reaped")


def supervise(config: WorkerConfig) -> int:
    if not isinstance(config, WorkerConfig):
        raise TypeError("Expected WorkerConfig")
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("Supervised worker cannot run inside an event loop")
    if os.name != "posix" or threading.current_thread() is not threading.main_thread():
        raise RuntimeError("Supervised worker requires a dedicated POSIX main-thread entrypoint")
    old_reaper = subreaper(True)
    rfd, wfd = os.pipe()
    env = dict(os.environ)
    env[OPTIONS] = json.dumps({"config": config.to_dict(), "owner": str(uuid4())})
    env[STATUS_FD] = str(wfd)
    child = None
    selector = selectors.DefaultSelector()
    old_handlers = {}
    requested_stop = False
    stop_deadline = None
    ready = False
    born = time.monotonic()
    last = born
    buffer = b""
    code = FAULT

    def stop(signum: int, frame: Any) -> None:
        nonlocal requested_stop
        requested_stop = True

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            old_handlers[sig] = signal.signal(sig, stop)
        child = subprocess.Popen(
            [sys.executable, "-m", "dewey.worker._process"],
            env=env,
            stdin=subprocess.DEVNULL,
            pass_fds=(wfd,),
            start_new_session=True,
        )
        assert os.getpgid(child.pid) == child.pid
        os.close(wfd)
        wfd = -1
        selector.register(rfd, selectors.EVENT_READ)
        while True:
            now = time.monotonic()
            if child.poll() is not None:
                code = 0 if requested_stop and child.returncode == 0 else FAULT
                break
            if requested_stop and stop_deadline is None:
                stop_deadline = now + config.shutdown_seconds + config.timing.termination_seconds
                os.kill(child.pid, signal.SIGTERM)  # Drain parent, not immediate child kill.
            if stop_deadline is not None:
                if now >= stop_deadline:
                    break
            elif now >= (
                last + config.supervision_seconds if ready else born + config.startup_seconds
            ):
                LOG.error("Native service missed its independent supervision deadline")
                break
            for _, _events in selector.select(timeout=0.02):
                chunk = os.read(rfd, 4096)
                if not chunk:
                    selector.unregister(rfd)
                    continue  # Popen exit, not EOF alone, decides the lifecycle.
                buffer += chunk
                if len(buffer) > 65536:
                    raise RuntimeError("Invalid worker progress protocol")
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    phase, stamp = line.decode("ascii").split()
                    sent = float(stamp)
                    if phase not in ("ready", "progress") or not 0 <= sent <= time.monotonic():
                        raise RuntimeError("Invalid worker progress protocol")
                    ready |= phase == "ready"
                    last = max(last, sent)
    except BaseException as exc:
        LOG.error("Worker supervisor failed (%s)", type(exc).__name__)
    finally:
        if child is not None:
            try:
                contain(child, config.timing.termination_seconds)
            except BaseException as exc:
                code = CLEANUP_FAILURE
                LOG.error(
                    "Worker containment uncertain (%s); do not restart until enclosing runtime containment",
                    type(exc).__name__,
                )
        selector.close()
        os.close(rfd)
        if wfd != -1:
            os.close(wfd)
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        if old_reaper is not None:
            subreaper(bool(old_reaper))
    return code


if __name__ == "__main__":
    raise SystemExit(service())
