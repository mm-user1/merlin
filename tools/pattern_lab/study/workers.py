"""A bounded spawn pool for the top-level numerical instrument job.

The coordinator still owns instrument selection, every pack read, every output
write and the evidence semantics.  This module owns only transport: a small
number of explicit ``spawn`` processes, two bounded queues, opaque job keys and
a cleanup policy with a deadline.  It adds no task types, no callable registry
and no configurable executor framework.

Why explicit ``Process`` handles rather than a higher-level pool: on the
supported Python version, cancelling a future does not cancel a running call and
returning from ``shutdown(wait=False)`` is not process termination, so an owned,
bounded, forceful cleanup needs the documented process API directly.  A worker
is never replaced and a job is never retried.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import multiprocessing
import os
import pickle
import queue as queue_module
import signal
import sys
import threading
import time
import traceback
from typing import Any, Iterator, Sequence

from .. import PatternLabDataError

START_METHOD = "spawn"
# Graceful cleanup is attempted first, but never beyond this overall deadline.
CLEANUP_DEADLINE_SECONDS = 10.0
TERMINATE_JOIN_SECONDS = 2.0
KILL_JOIN_SECONDS = 2.0
# A generous hard outer bound on worker startup and on waiting for a result: a
# failure detector, not a performance assertion.
START_DEADLINE_SECONDS = 300.0
RESULT_DEADLINE_SECONDS = 3600.0
_POLL_SECONDS = 0.05
# How long teardown waits for the result pump to notice it should stop before
# abandoning it; the pump is a daemon thread, so waiting longer buys nothing.
_PUMP_JOIN_SECONDS = 0.5

# Startup variables that must be set before a child can import NumPy or BLAS.
THREAD_ENVIRONMENT = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "ARROW_IO_THREADS",
)

# One temporary process-global environment change at a time inside this tool.
# Concurrent independent study startups from unrelated threads of an embedding
# application are not supported; this is a documented limitation, not an
# environment service.
_ENVIRONMENT_LOCK = threading.Lock()


class WorkerJobError(PatternLabDataError):
    """One instrument job raised inside a worker; the child traceback is kept."""

    def __init__(self, *args, error_code: str | None = None, worker_traceback: str = ""):
        super().__init__(*args, error_code=error_code)
        self.worker_traceback = worker_traceback


class WorkerLostError(PatternLabDataError):
    """A worker exited without returning the result of a job it had started."""

    def __init__(self, *args, error_code: str | None = None, orphaned: Sequence[str] = ()):
        super().__init__(*args, error_code=error_code)
        self.orphaned = tuple(orphaned)


class WorkerTransportError(PatternLabDataError):
    """The pool's own result transport stopped while the pool was still open.

    This is a run-level failure that belongs to no instrument: the coordinator
    can no longer receive any result, so waiting for the outer result deadline
    would only delay an already certain failure.
    """


@dataclass(frozen=True)
class WorkerSettings:
    """The serializable settings one worker needs to initialize itself.

    ``frozen`` carries the coordinator's own :class:`LoadedExtension` records
    for the same declarations, so a child checks every declared module *and
    helper* file against the generation the coordinator froze rather than
    against its own freshly observed hashes.  This is private transport: it may
    change without any public request-schema change.
    """

    extensions: tuple[Any, ...]
    required: tuple[tuple[str, str], ...]
    frozen: tuple[Any, ...]


# --------------------------------------------------------------------------
# numerical thread policy
# --------------------------------------------------------------------------

@contextmanager
def single_threaded_children() -> Iterator[dict[str, str | None]]:
    """Pin child numerical-library thread counts to one for a pool's lifetime.

    The variables are set in the caller's environment *before* a child can
    import NumPy or BLAS, because an initializer that runs after those imports
    cannot reliably retune an already loaded library.  The scope covers the
    whole pool, including any lazy initial spawn, and the caller's previous
    mapping — including the absence of a variable — is restored on every exit
    path, after teardown.
    """
    with _ENVIRONMENT_LOCK:
        saved = {name: os.environ.get(name) for name in THREAD_ENVIRONMENT}
        for name in THREAD_ENVIRONMENT:
            os.environ[name] = "1"
        try:
            yield dict(saved)
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def configure_child_threads() -> dict[str, Any]:
    """Apply and observe a worker's own thread policy.

    The inherited startup environment is what actually constrains BLAS; the
    Arrow pools are additionally set through their public runtime API.  Only
    what can genuinely be read back is reported: a thread count is never
    invented, and no dependency is added merely to print one.
    """
    observed: dict[str, Any] = {
        "environment": {name: os.environ.get(name) for name in THREAD_ENVIRONMENT},
        "pyarrow": None,
        "notes": (
            "Environment variables are startup policy evidence. BLAS runtime thread limits are "
            "not observable from Python here and are not claimed to be measured."
        ),
    }
    pyarrow = sys.modules.get("pyarrow")
    if pyarrow is None:
        try:
            import pyarrow  # noqa: PLC0415 - optional at this point in startup
        except ImportError:
            return observed
    try:
        pyarrow.set_cpu_count(1)
        pyarrow.set_io_thread_count(1)
        observed["pyarrow"] = {
            "cpu_count": int(pyarrow.cpu_count()),
            "io_thread_count": int(pyarrow.io_thread_count()),
        }
    except Exception as exc:  # pragma: no cover - reported, never fatal
        observed["pyarrow"] = {"error": f"{type(exc).__name__}: {exc}"}
    return observed


# --------------------------------------------------------------------------
# the child
# --------------------------------------------------------------------------

def _error_record(exc: BaseException) -> dict[str, Any]:
    """A structured, always serializable description of a child failure."""
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "error_code": getattr(exc, "error_code", None),
        "traceback": traceback.format_exc(),
    }


def initialize_worker(settings: WorkerSettings) -> dict[str, Any]:
    """Register the built-ins and the declared extension set inside a worker.

    ``register_builtins()`` is called explicitly and idempotently rather than
    relying on a package import order to populate the registry.  Every declared
    module *and helper* file is then checked against the coordinator's frozen
    generation **before** anything is imported or registered, and again after
    import, so neither a helper-only edit nor an import-time edit can become a
    freshly accepted generation here.  The child never substitutes its own
    observed hashes for the generation the coordinator froze.
    """
    from . import builtins as study_builtins
    from . import contracts
    from . import extensions as study_extensions
    from . import validation as study_validation

    study_builtins.register_builtins()
    study_extensions.require_frozen_generation(
        settings.extensions, settings.frozen, where="worker initialization"
    )
    loaded = study_extensions.load_extensions(settings.extensions)
    study_extensions.require_frozen_generation(
        settings.extensions, settings.frozen, where="worker initialization after import"
    )
    absent = [
        f"{kind} {identifier!r}"
        for kind, identifier in settings.required
        if identifier not in contracts.registered(kind)
    ]
    if absent:
        raise PatternLabDataError(
            "worker initialization: the required descriptors " + ", ".join(sorted(absent))
            + " are not registered in this worker. A built-in or declared custom registration is "
            "missing.",
            error_code="worker_initialization_failed",
        )
    used = [contracts.registration(kind, identifier) for kind, identifier in settings.required]
    study_validation.require_declared_sources(
        used, study_validation.declared_digests(loaded), where="worker initialization"
    )
    return {"extensions": [record.module for record in loaded]}


def _worker_main(settings: WorkerSettings, tasks, results) -> None:
    """One worker: initialize once, then run opaque jobs until told to stop."""
    try:
        # The coordinator owns handled Ctrl+C; a worker must not race it to
        # publish an interrupted status. This is installed before any trusted
        # extension is imported, and covers an interrupted startup too.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (ValueError, OSError):  # pragma: no cover - platform dependent
        pass
    # A printing extension must not corrupt the coordinator's machine-readable
    # stdout. Diagnostics are not research output.
    sys.stdout = sys.stderr
    threads = configure_child_threads()
    try:
        registered = initialize_worker(settings)
    except BaseException as exc:  # noqa: BLE001 - reported through IPC, never printed
        try:
            results.put(("init_failed", None, _error_record(exc)))
        except BaseException:  # pragma: no cover - the channel is already gone
            pass
        return
    results.put(("ready", None, {"pid": os.getpid(), "threads": threads, **registered}))

    from .job import run_instrument_job

    while True:
        try:
            item = tasks.get()
        except (EOFError, OSError, KeyboardInterrupt):  # pragma: no cover - teardown races
            return
        if item is None:
            return
        job_id, blob = item
        try:
            results.put(("started", job_id, os.getpid()))
            payload = pickle.loads(blob)
            del blob
            result = run_instrument_job(payload)
            del payload
            encoded = pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)
            del result
        except BaseException as exc:  # noqa: BLE001 - every failure is reported, not raised
            try:
                results.put(("error", job_id, _error_record(exc)))
            except BaseException:  # pragma: no cover - the channel is already gone
                return
            continue
        results.put(("result", job_id, encoded))


# --------------------------------------------------------------------------
# the coordinator-side pool
# --------------------------------------------------------------------------

class SpawnJobPool:
    """A fixed set of spawn workers, keyed by opaque job IDs.

    The pool never decides what a job means: the coordinator submits an opaque
    key and an already prepared payload, and receives back that key with either
    a result or a structured failure.
    """

    def __init__(self, worker_count: int, settings: WorkerSettings) -> None:
        self._context = multiprocessing.get_context(START_METHOD)
        self._settings = settings
        self._count = int(worker_count)
        self._tasks = None
        self._results = None
        # Results are pumped off the IPC channel into this ordinary in-process
        # queue, so the coordinator always waits with a deadline it controls.
        self._inbox: queue_module.Queue = queue_module.Queue()
        self._pump: threading.Thread | None = None
        # How the pump exited, if it has.  Only teardown may legitimately end
        # it, so any value observed while the pool is open is a transport
        # failure rather than a reason to wait for the outer deadline.
        self._pump_exit: str | None = None
        self._workers: list[Any] = []
        self._ready: list[dict[str, Any]] = []
        self._owner: dict[str, int] = {}
        self._pending: set[str] = set()
        self._closed = False

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Start every worker and wait until each has initialized successfully."""
        self._tasks = self._context.Queue()
        self._results = self._context.Queue()
        self._pump = threading.Thread(
            target=self._pump_results, name="pattern-lab-study-results", daemon=True
        )
        self._pump.start()
        for index in range(self._count):
            worker = self._context.Process(
                target=_worker_main,
                args=(self._settings, self._tasks, self._results),
                name=f"pattern-lab-study-worker-{index}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)
        deadline = time.monotonic() + START_DEADLINE_SECONDS
        while len(self._ready) < self._count:
            message = self._receive(deadline, where="worker startup")
            kind, _job_id, payload = message
            if kind == "ready":
                self._ready.append(payload)
            elif kind == "init_failed":
                raise PatternLabDataError(
                    "worker initialization failed: "
                    f"{payload['type']}: {payload['message']}",
                    error_code=payload.get("error_code") or "worker_initialization_failed",
                )
            else:  # pragma: no cover - no job can exist before startup finishes
                raise PatternLabDataError(
                    f"worker startup received an unexpected {kind!r} message.",
                    error_code="worker_initialization_failed",
                )

    def _pump_results(self) -> None:
        """Move results off the IPC channel so the coordinator never blocks on it.

        A worker killed mid-message can leave a truncated frame, and a blocking
        read of that channel is then unrecoverable — no timeout helps once the
        length prefix has been consumed.  Only this daemon thread can be caught
        by that.  The coordinator waits on an ordinary in-process queue with its
        own deadline and liveness checks, and teardown abandons the channel
        rather than draining it.

        A *blocked* pump is not an *exited* pump: this records only how it
        actually finished, so :meth:`_check_pump` can turn an unexpected exit
        into an actionable transport failure while worker-loss detection and
        the outer deadlines keep covering the blocked case.
        """
        results = self._results
        try:
            while True:
                message = results.get()
                if message is None:
                    self._pump_exit = "the teardown sentinel was received"
                    return
                self._inbox.put(message)
        except BaseException as exc:  # noqa: BLE001 - reported, never raised into teardown
            self._pump_exit = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
        finally:
            if self._pump_exit is None:  # pragma: no cover - defensive
                self._pump_exit = "the pump thread returned without a reported reason"

    @property
    def worker_pids(self) -> list[int]:
        return [int(item["pid"]) for item in self._ready]

    @property
    def thread_evidence(self) -> list[dict[str, Any]]:
        """What each initialized child actually observed about its thread policy."""
        return [dict(item["threads"], pid=int(item["pid"])) for item in self._ready]

    def live_workers(self) -> list[int]:
        return [int(worker.pid) for worker in self._workers if worker.is_alive()]

    # -- submission and results -------------------------------------------

    def submit(self, job_id: str, payload: Any) -> None:
        """Serialize and transport one prepared job.

        The payload is encoded here rather than by the queue's feeder thread, so
        a serialization failure is raised synchronously and is attributable to
        this job instead of surfacing asynchronously as a lost task.
        """
        if self._closed:
            raise PatternLabDataError("the worker pool is already shut down.")
        try:
            blob = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        except BaseException as exc:
            raise WorkerJobError(
                f"{job_id}: the prepared job payload could not be serialized for transport: "
                f"{type(exc).__name__}: {exc}",
                error_code="job_failed",
                worker_traceback=traceback.format_exc(),
            ) from exc
        self._pending.add(job_id)
        self._tasks.put((job_id, blob))

    def poll(self) -> tuple[str, str, Any] | None:
        """Return one ready result or failure without blocking, or ``None``.

        The pump is checked on the empty branch too, so an observed transport
        failure stops new admission and dispatch as well as waiting.
        """
        while True:
            try:
                message = self._inbox.get_nowait()
            except queue_module.Empty:
                self._check_pump(where="job execution")
                return None
            handled = self._interpret(message)
            if handled is not None:
                return handled

    def take(self, *, timeout: float = RESULT_DEADLINE_SECONDS) -> tuple[str, str, Any]:
        """Block until one job finishes, or until a worker is lost."""
        deadline = time.monotonic() + timeout
        while True:
            message = self._receive(deadline, where="job execution")
            handled = self._interpret(message)
            if handled is not None:
                return handled

    def _interpret(self, message) -> tuple[str, str, Any] | None:
        kind, job_id, payload = message
        if kind == "started":
            self._owner[job_id] = int(payload)
            return None
        if kind == "result":
            self._pending.discard(job_id)
            self._owner.pop(job_id, None)
            return ("result", job_id, pickle.loads(payload))
        if kind == "error":
            self._pending.discard(job_id)
            self._owner.pop(job_id, None)
            return ("error", job_id, payload)
        if kind == "init_failed":  # pragma: no cover - startup already returned
            raise PatternLabDataError(
                f"worker initialization failed: {payload['type']}: {payload['message']}",
                error_code=payload.get("error_code") or "worker_initialization_failed",
            )
        return None  # pragma: no cover - 'ready' only arrives during startup

    def _receive(self, deadline: float, *, where: str):
        """Wait for one message, treating an abrupt child exit as a failure."""
        while True:
            try:
                return self._inbox.get(timeout=_POLL_SECONDS)
            except queue_module.Empty:
                pass
            lost = [worker for worker in self._workers if not worker.is_alive()]
            if lost:
                # A worker only exits on its sentinel, so an exit here is an
                # anomaly. Give a message that crossed just before the exit one
                # bounded chance, then fail rather than wait for a result that
                # may never arrive.
                try:
                    return self._inbox.get(timeout=_POLL_SECONDS * 8)
                except queue_module.Empty:
                    raise self._lost_error(lost, where=where) from None
            # A dead pump has already delivered everything it ever will, so no
            # grace window is needed and none is taken.
            self._check_pump(where=where)
            if time.monotonic() >= deadline:
                raise PatternLabDataError(
                    f"{where}: no worker message arrived within the hard outer deadline; the run "
                    "is stopped rather than waiting indefinitely.",
                    error_code="worker_timeout",
                )

    def _check_pump(self, *, where: str) -> None:
        """Fail promptly when the result pump stopped while the pool is open.

        Only teardown ends the pump, and a thread reports itself dead only once
        its last delivery has been made, so an exit observed here means no
        further result can ever arrive.  Waiting for the outer result deadline
        would just postpone a certain failure.
        """
        if self._closed or self._pump is None or self._pump.is_alive():
            return
        raise WorkerTransportError(
            f"{where}: the result pump stopped while this run's workers were still active "
            f"({self._pump_exit}), so no further result can be received. The run is stopped "
            "rather than waiting for the hard outer deadline; no worker is replaced, no job is "
            "retried and the transport is not restarted.",
            error_code="transport_failed",
        )

    def _lost_error(self, lost: Sequence[Any], *, where: str) -> WorkerLostError:
        pids = sorted(int(worker.pid) for worker in lost if worker.pid is not None)
        codes = sorted({worker.exitcode for worker in lost})
        orphaned = sorted(
            job_id for job_id in self._pending if self._owner.get(job_id) in pids
        )
        detail = (
            f" It had started {orphaned}." if orphaned
            else " It had not started any job that is still pending."
        )
        return WorkerLostError(
            f"{where}: worker process(es) {pids} exited unexpectedly with exit code(s) {codes}."
            + detail
            + " No worker is replaced and no job is retried.",
            error_code="worker_lost",
            orphaned=orphaned,
        )

    # -- teardown ----------------------------------------------------------

    def shutdown(self, *, abort: bool) -> dict[str, Any]:
        """Stop this run's workers within a bounded overall deadline.

        The pump thread keeps consuming the result channel throughout, so a
        producer is never left blocked flushing a large message before a join.
        A graceful stop sends one sentinel per worker and joins within the
        deadline; whatever is still owed is then terminated and, if necessary,
        killed.  During an abort the received results are discarded rather than
        published, and a channel whose producer was lost or terminated is
        abandoned instead of drained.
        """
        report = {"terminated": [], "killed": [], "drained": 0, "abort": bool(abort)}
        if self._closed:
            return report
        self._closed = True
        deadline = time.monotonic() + CLEANUP_DEADLINE_SECONDS
        try:
            if not abort:
                for _worker in self._workers:
                    try:
                        self._tasks.put(None)
                    except BaseException:  # pragma: no cover - the channel is gone
                        break
                for worker in self._workers:
                    remaining = max(0.0, deadline - time.monotonic())
                    worker.join(timeout=remaining if remaining > 0 else 0.0)
            for worker in self._workers:
                if worker.is_alive():
                    worker.terminate()
                    report["terminated"].append(int(worker.pid))
            for worker in self._workers:
                worker.join(timeout=TERMINATE_JOIN_SECONDS)
                if worker.is_alive():
                    worker.kill()
                    report["killed"].append(int(worker.pid))
                    worker.join(timeout=KILL_JOIN_SECONDS)
            report["drained"] = self._discard()
        finally:
            self._stop_pump()
            self._close_queues()
        return report

    def _discard(self) -> int:
        """Drop whatever the pump already moved across; it is never published."""
        count = 0
        while True:
            try:
                self._inbox.get_nowait()
            except queue_module.Empty:
                return count
            count += 1

    def _stop_pump(self) -> None:
        """Ask the pump to finish, then bounded-join it and move on regardless."""
        if self._pump is None:
            return
        try:
            self._results.put(None)
        except BaseException:  # pragma: no cover - the channel is already gone
            pass
        # The pump is a daemon thread: if a truncated frame has it stuck, the
        # coordinator abandons it rather than waiting.
        self._pump.join(timeout=_PUMP_JOIN_SECONDS)
        self._pump = None

    def _close_queues(self) -> None:
        for channel in (self._tasks, self._results):
            if channel is None:
                continue
            try:
                # Never block on a feeder thread whose consumer is gone.
                channel.cancel_join_thread()
            except BaseException:  # pragma: no cover - already closed
                pass
            try:
                channel.close()
            except BaseException:  # pragma: no cover - already closed
                pass
        self._tasks = None
        self._results = None
