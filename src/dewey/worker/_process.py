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
from dewey._worker.pool import PoolBroken, WorkerPool
from dewey._worker.runner import NativeRunner
from dewey._worker.runtime import LedgerRuntime
from dewey.alerts import load_alert_hook
from dewey.core.execution import configure_failure_details
from dewey.core.tracing import configure_tracing
from dewey.policy import registry
from dewey.worker import WorkerApplication, WorkerConfig, WorkerContext

LOG = logging.getLogger("dewey.worker")
STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)
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
    configure_failure_details(ctx.config.failure_details)
    configure_tracing(ctx.config.tracing)
    app = application(ctx)
    # The worker gives a stopping task process termination_seconds to close
    # (see service()); the span flush must fit inside it, with a margin.
    flush_seconds = max(0.1, ctx.config.timing.termination_seconds * 0.8)
    return LedgerRuntime(
        app.storage, close_resources=app.close_resources, flush_seconds=flush_seconds
    )


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
    # A forked service kept the stop signals blocked until this point (see
    # fork_service): one that arrived meanwhile is delivered now, to this handler.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, STOP_SIGNALS)
    runner = None
    app = None
    code = FAULT
    try:
        load_alert_hook(config.alert_hook)
        configure_failure_details(config.failure_details)
        try:
            configure_tracing(config.tracing)  # fail here, not in every task process
        except RuntimeError as exc:
            LOG.error("Native service cannot start: %s", exc)
            return FAULT
        app = application(ctx)
        pulse()  # The application is imported: startup progress, not yet readiness.
        if not registry.task_types():
            LOG.warning("No task handlers are registered; this worker will claim nothing")
        pool = WorkerPool(
            "dewey.worker._process:create_runtime",
            concurrency=config.concurrency,
            task_timeout_seconds=config.timing.task_timeout_seconds,
            startup_timeout_seconds=config.startup_seconds,
            termination_timeout_seconds=config.timing.termination_seconds / 4,
            # Idle task processes get the whole termination budget, together,
            # to close (a span flush is the usual reason) before being stopped.
            cleanup_timeout_seconds=config.timing.termination_seconds,
            max_restarts=config.max_restarts,
            start_method=config.start_method,
            progress=pulse,
            close_in_children=(fd,),  # the status pipe is the service's alone
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
                    pool.stop_starting()  # drain with the children already started
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
    except PoolBroken as exc:
        LOG.error("Native service failed (%s): %s%s", type(exc).__name__, exc, startup_hint(exc))
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


def startup_hint(exc: BaseException) -> str:
    """The worker-level setting behind a pool startup timeout, for the operator."""
    if "initialization timed out" in str(exc):
        return "; raise startup_seconds or give the worker more CPU"
    return ""


def group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # macOS answers EPERM, not ESRCH, for a group whose every member is a
        # zombie waiting for launchd to reap it. The group is ours, so no live
        # member can be one we may not signal.
        if sys.platform == "darwin":
            return False
        raise


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


class ForkedService:
    """The service forked from this process: the subset of Popen that supervision uses."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.returncode: int | None = None

    def poll(self) -> int | None:
        if self.returncode is None:
            try:
                pid, status = os.waitpid(self.pid, os.WNOHANG)
            except ChildProcessError:
                return self.returncode
            if pid == self.pid:
                self.returncode = os.waitstatus_to_exitcode(status)
        return self.returncode

    def wait(self, timeout: float) -> int:
        until = time.monotonic() + timeout
        while self.poll() is None:
            if time.monotonic() >= until:
                raise subprocess.TimeoutExpired("dewey.worker._process", timeout)
            time.sleep(0.01)
        assert self.returncode is not None
        return self.returncode


def fork_service(env: dict[str, str], wfd: int, rfd: int) -> ForkedService:
    """Run service() in a forked child that leads its own session, like the spawned one.

    The child shares this process's imported application copy-on-write. It must
    not inherit anything this process has to itself: Dewey opened no connection
    or thread before this point, so what crosses is the caller's own.
    """
    sync_r, sync_w = os.pipe()
    # The child inherits the supervisor's stop handler, whose closure in the
    # child is a dead copy: a stop signal before service() installs its own
    # handler would be lost, and the drain would wait for SIGKILL. Block the
    # signals across the fork; the child resets the handlers at once and keeps
    # the signals blocked until service() has installed its own, so a stop that
    # arrives in between is delivered then, to the right handler.
    blocked = signal.pthread_sigmask(signal.SIG_BLOCK, STOP_SIGNALS)
    try:
        pid = os.fork()
    except BaseException:
        # No child: give the caller its mask back, and the handshake pipe.
        signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
        os.close(sync_r)
        os.close(sync_w)
        raise
    if pid == 0:
        code = FAULT
        try:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.default_int_handler)
            os.setsid()
            os.close(sync_r)
            os.close(rfd)
            os.environ.update(env)
            devnull = os.open(os.devnull, os.O_RDONLY)
            os.dup2(devnull, 0)
            os.close(devnull)
            os.close(sync_w)  # The session exists: the supervisor may now sign.
            code = service()
        except BaseException:
            code = FAULT
        finally:
            # os._exit runs no atexit hooks, so the host's logging handlers would
            # not flush: the lines explaining an exit 70 or 71 could be the ones
            # lost. Flush and close them here; other atexit hooks still do not run.
            try:
                logging.shutdown()
            except Exception:
                pass
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            finally:
                os._exit(code)
    signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
    os.close(sync_w)
    try:
        if os.read(sync_r, 1) != b"":
            raise RuntimeError("Invalid worker fork handshake")
    finally:
        os.close(sync_r)
    return ForkedService(pid)


def contain(child: subprocess.Popen[Any] | ForkedService, seconds: float) -> None:
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
        if config.start_method == "fork":
            child = fork_service(env, wfd, rfd)
        else:
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
            elif ready and now >= last + config.supervision_seconds:
                LOG.error(
                    "Native service missed its independent supervision deadline: no progress "
                    "for supervision_seconds=%g",
                    config.supervision_seconds,
                )
                break
            elif not ready and now >= last + config.startup_seconds:
                # Startup is paced by progress, not by a single absolute deadline:
                # importing the application and starting each task process report
                # progress, so a slow CPU fails only when one of them stalls.
                LOG.error(
                    "Native service did not start: no progress for startup_seconds=%g "
                    "(%.0fs since launch); raise startup_seconds if the application "
                    "takes longer to import on this CPU",
                    config.startup_seconds,
                    now - born,
                )
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
