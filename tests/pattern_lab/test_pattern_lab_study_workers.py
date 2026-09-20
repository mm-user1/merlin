"""The bounded spawn pool: parity, bounds, isolation, failure and cleanup.

These cases start real ``spawn`` children through the production coordinator.
Pytest ``monkeypatch`` state is not inherited by a spawned interpreter, so the
child-side guards live in a declared temporary test extension that installs
them only when it detects it is running inside a worker; the parent's own
behavior is left untouched and no production test hook exists.

Handshakes are explicit: a delayed job waits for a marker this test creates
after observing another job's publication, rather than sleeping and hoping.
Every process fixture has a generous hard outer timeout — a failure detector,
not a performance assertion — and cleans up precisely the processes and
storage it owns.
"""

from __future__ import annotations

import gc
import json
import os
from pathlib import Path
import queue as queue_module
import signal
import subprocess
import sys
import textwrap
import time
import weakref

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabStudyError
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.study import evidence as study_evidence
from tools.pattern_lab.study import extensions as study_extensions
from tools.pattern_lab.study import runner as study_runner
from tools.pattern_lab.study import workers as study_workers

from ._helpers import (
    TWO_GREEN_EVERY_BAR,
    TWO_GREEN_STATE_ENTRY,
    fixed_horizon_model,
    instrument_source,
    publish,
    study_group_ms,
    study_protocol,
    study_request,
    timeframe_bars,
)

pytestmark = pytest.mark.slow

TIMEFRAME = 30
GROUPS = 16
# A generous hard outer bound for a whole process scenario on a small host.
SCENARIO_TIMEOUT = 300.0


def rising(count: int = GROUPS, base: float = 100.0):
    return tuple(
        (base + index, base + 2.0 + index, base - 1.0 + index, base + 1.0 + index, 10.0 + index)
        for index in range(count)
    )


def falling(count: int = GROUPS):
    return tuple(
        (100.0 - index, 100.5 - index, 97.0 - index, 99.0 - index, 10.0 + index)
        for index in range(count)
    )


def build_pack(root: Path, instruments, *, groups: int = GROUPS):
    sources = []
    for contract, options in instruments.items():
        stamps, values = timeframe_bars(
            TIMEFRAME,
            options.get("specs", rising(groups)),
            drop_groups=options.get("drop_groups", ()),
        )
        sources.append(
            instrument_source(
                stamps, values, symbol=contract.split("-")[0], venue="TEST", contract=contract
            )
        )
    publish(root, sources)
    return root


def request_for(instrument_ids=None, *, hypotheses=None, models=None, metrics=None,
                extensions=None, timeframes=(TIMEFRAME,), start_group: int = 2,
                end_group: int = 12, warmup_group: int = 0, coverage_groups: int = GROUPS + 8):
    return study_request(
        protocol=study_protocol(
            first_ms=study_group_ms(warmup_group, TIMEFRAME),
            coverage_end_ms=study_group_ms(coverage_groups, TIMEFRAME),
        ),
        start_ms=study_group_ms(start_group, TIMEFRAME),
        end_ms=study_group_ms(end_group, TIMEFRAME),
        warmup_ms=study_group_ms(warmup_group, TIMEFRAME),
        timeframes=list(timeframes),
        hypotheses=list(hypotheses if hypotheses is not None else [TWO_GREEN_EVERY_BAR]),
        models=list(models if models is not None else [fixed_horizon_model(TIMEFRAME, [30, 60])]),
        metrics=metrics,
        extensions=extensions,
        instruments=None if instrument_ids is None else {"ids": list(instrument_ids)},
    )


# --------------------------------------------------------------------------
# the declared temporary test extension
# --------------------------------------------------------------------------

WORKER_EXTENSION = '''\
"""A declared temporary test extension with child-local guards.

It installs its guards only inside a spawned worker, detected through the
standard ``multiprocessing.parent_process()`` API, so the parent coordinator is
never affected and no production test mode exists.
"""

import multiprocessing
import os
import signal
import sys
import time

import numpy as np

from tools.pattern_lab.study import contracts
from tools.pattern_lab.study.contracts import (
    ModelCase,
    ModelDescriptor,
    ModelEvidence,
    OutcomeSpec,
)

IN_WORKER = multiprocessing.parent_process() is not None
HANDSHAKE_TIMEOUT_SECONDS = 120.0

REQUIRED = (
    ("hypothesis", "two_green_rising_quote_volume"),
    ("model", "fixed_horizon_path"),
    ("model", "{model_id}"),
)


def _install_child_guards():
    """Deny networking, pack access and output publication inside a worker."""
    import socket

    from tools.pattern_lab import data as pack_data
    from tools.pattern_lab import pack_lock
    from tools.pattern_lab.study import evidence as study_evidence

    def refuse(what):
        def guard(*args, **kwargs):
            raise AssertionError("a worker attempted " + what)
        return guard

    socket.socket.connect = refuse("a network connect")
    socket.socket.connect_ex = refuse("a network connect")
    socket.create_connection = refuse("a network connection")
    pack_data.load_slice = refuse("a pack read")
    pack_data.read_session = refuse("a pack read session")
    pack_data.inspect_pack = refuse("a pack inspection")
    pack_lock.pack_guard = refuse("the pack lock")
    study_evidence.publish_job = refuse("evidence publication")


if IN_WORKER:
    _install_child_guards()


def _validate(settings, timeframes):
    contracts.closed_keys(
        settings, ("mode", "marker", "signal_marker", "only_instrument"), "settings"
    )
    return {
        "mode": str(settings["mode"]),
        "marker": str(settings.get("marker", "")),
        "signal_marker": str(settings.get("signal_marker", "")),
        "only_instrument": str(settings.get("only_instrument", "")),
    }


def _cases(settings, timeframe):
    return (
        ModelCase(
            case_id="probe",
            timeframe_minutes=int(timeframe),
            parameters={},
            outcomes=(
                OutcomeSpec("worker_pid", "count", "the PID that produced this row"),
                OutcomeSpec("registered", "count", "required descriptors found in this process"),
            ),
            primary=True,
        ),
    )


def _wait_for(path, signal_path):
    """Block until the fixture releases this job, with a hard outer timeout."""
    if signal_path:
        open(signal_path, "w").close()
    deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise AssertionError("the test handshake marker never appeared: " + path)
        time.sleep(0.02)


def _evaluate(series, settings, anchors):
    only = settings["only_instrument"]
    # The behavior is keyed by instrument, so one job can be delayed or broken
    # while the others run normally.
    mode = settings["mode"] if (not only or series.instrument_id == only) else "ok"
    if mode == "network":
        import socket

        socket.socket().connect(("127.0.0.1", 9))
    elif mode == "pack_read":
        from tools.pattern_lab import data as pack_data

        pack_data.load_slice("TEST_AAA-USDT-SWAP", start=0, end=1)
    elif mode == "publish":
        from tools.pattern_lab.study import evidence as study_evidence

        study_evidence.publish_job("/nowhere", "TEST_AAA-USDT-SWAP", tables={}, stats={})
    elif mode == "mutate_input":
        series.values[0, 0] = -1.0
    elif mode == "raise":
        raise ValueError("synthetic numerical failure inside the job")
    elif mode == "abrupt_exit":
        if settings["signal_marker"]:
            open(settings["signal_marker"], "w").close()
        os._exit(7)
    elif mode == "print":
        print("noise on stdout from a trusted extension")
        sys.stdout.flush()
    elif mode == "wait":
        _wait_for(settings["marker"], settings["signal_marker"])
    elif mode == "sigint_probe":
        # The policy itself is observable on every platform; a POSIX fixture
        # additionally delivers a real signal while this job waits.
        if signal.getsignal(signal.SIGINT) is not signal.SIG_IGN:
            raise AssertionError("the worker did not ignore SIGINT before executing extensions")
        _wait_for(settings["marker"], settings["signal_marker"])

    found = float(sum(
        1 for kind, identifier in REQUIRED if identifier in contracts.registered(kind)
    ))
    count = int(anchors.rows.size)
    # The PID is execution provenance, so it is reported only when a case
    # explicitly asks for it; otherwise the outcome is an explicit null and two
    # runs of the same study stay comparable.
    if mode == "identify":
        pids = np.full(count, float(os.getpid()), dtype=np.float64)
        pid_reason = np.full(count, "available", dtype=object)
    else:
        pids = np.full(count, np.nan, dtype=np.float64)
        pid_reason = np.full(count, "pid_not_reported", dtype=object)
    return ModelEvidence(
        kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
        rows={
            "case_id": np.full(count, "probe", dtype=object),
            "anchor_open_ms": anchors.open_ms.astype(np.int64),
            "worker_pid": pids,
            "worker_pid__reason": pid_reason,
            "registered": np.full(count, found, dtype=np.float64),
            "registered__reason": np.full(count, "available", dtype=object),
        },
    )


def register(context):
    context.register_model(
        ModelDescriptor(
            model_id="{model_id}",
            version="1",
            validate_settings=_validate,
            resolve_cases=_cases,
            evaluate=_evaluate,
        )
    )
'''

# A module whose registration fails only inside a worker: the coordinator
# imports it successfully, so the failure is a worker initialization error.
FAILING_INITIALIZER = '''\
import multiprocessing

from tools.pattern_lab.study import contracts
from tools.pattern_lab.study.contracts import ModelCase, ModelDescriptor, ModelEvidence, OutcomeSpec

import numpy as np


def _validate(settings, timeframes):
    contracts.closed_keys(settings, (), "settings")
    return {}


def _cases(settings, timeframe):
    return (
        ModelCase("probe", int(timeframe), {}, (OutcomeSpec("value", "count"),), primary=True),
    )


def _evaluate(series, settings, anchors):
    count = int(anchors.rows.size)
    return ModelEvidence(
        kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
        rows={
            "case_id": np.full(count, "probe", dtype=object),
            "anchor_open_ms": anchors.open_ms.astype(np.int64),
            "value": np.zeros(count, dtype=np.float64),
            "value__reason": np.full(count, "available", dtype=object),
        },
    )


def register(context):
    if multiprocessing.parent_process() is not None:
        raise RuntimeError("synthetic worker initialization failure")
    context.register_model(
        ModelDescriptor(
            model_id="{model_id}",
            version="1",
            validate_settings=_validate,
            resolve_cases=_cases,
            evaluate=_evaluate,
        )
    )
'''


def write_extension(root: Path, name: str, template: str = WORKER_EXTENSION) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{name}.py").write_text(
        template.replace("{model_id}", f"{name}_model"), encoding="utf-8"
    )
    return {"module": name, "source_root": str(root), "helpers": []}


def frozen_record(declaration, digests: dict) -> study_extensions.LoadedExtension:
    """One coordinator-side frozen source record, built with explicit digests."""
    return study_extensions.LoadedExtension(
        module=declaration.module,
        source_root=str(declaration.source_root),
        module_path=str(Path(declaration.source_root) / f"{declaration.module}.py"),
        files=tuple(sorted(digests.items())),
    )


def probe_model(name: str, *, mode: str = "ok", marker: Path | None = None,
                signal_marker: Path | None = None, only_instrument: str | None = None,
                instance_id: str = "probe") -> dict:
    settings = {"mode": mode}
    if marker is not None:
        settings["marker"] = str(marker)
    if signal_marker is not None:
        settings["signal_marker"] = str(signal_marker)
    if only_instrument is not None:
        settings["only_instrument"] = only_instrument
    return {"id": instance_id, "model": f"{name}_model", "settings": settings}


def semantic_summary(run_root) -> dict:
    """A run's summary without the physical provenance that may legitimately differ."""
    summary = json.loads(
        (Path(run_root) / study_evidence.SUMMARY_FILE).read_text(encoding="utf-8")
    )
    summary.pop("run_root", None)
    return summary


class ProcessCommandError(AssertionError):
    """A process query or termination command could not be trusted.

    An unavailable, refused or failed process tool says nothing about the
    process, so it is reported instead of being read as "the process is gone"
    or "cleanup succeeded".
    """


def _process_command(command, run) -> subprocess.CompletedProcess:
    try:
        return run(command, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProcessCommandError(
            f"{command[0]} could not be invoked ({type(exc).__name__}: {exc}); an unavailable "
            "process tool is not evidence about the process."
        ) from exc


def _command_detail(command, completed) -> str:
    return (
        f"{command[0]} returned {completed.returncode}; "
        f"stdout={(completed.stdout or '').strip()!r} stderr={(completed.stderr or '').strip()!r}"
    )


def _windows() -> bool:
    return os.name == "nt"


def live(pid: int, *, windows: bool | None = None, run=None) -> bool:
    """Is this PID still running? Never a side effect on either platform.

    ``os.kill(pid, 0)`` is a POSIX probe; on Windows that call terminates the
    target, so the supported query tool is used there instead and is never
    substituted.  A successful query saying the PID is absent and a failed
    ``tasklist`` invocation are different outcomes: only the first is an
    answer.  The decision is driven by the PID appearing in a matched row, so
    no localized "no tasks" message has to be recognized.
    """
    pid = int(pid)
    if _windows() if windows is None else windows:
        command = ["tasklist", "/FI", f"PID eq {pid}", "/NH"]
        completed = _process_command(command, run or subprocess.run)
        if completed.returncode != 0:
            raise ProcessCommandError(
                f"tasklist could not report PID {pid}: {_command_detail(command, completed)}. "
                "A failed query is not proof that the process is gone."
            )
        return str(pid) in (completed.stdout or "")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The process exists; this user simply may not signal it.
        return True
    return True


def force_kill(pid: int, *, windows: bool | None = None, run=None) -> None:
    """Terminate one owned process, or establish that it had already exited."""
    pid = int(pid)
    if _windows() if windows is None else windows:
        runner = run or subprocess.run
        command = ["taskkill", "/PID", str(pid), "/F", "/T"]
        completed = _process_command(command, runner)
        if completed.returncode == 0:
            return
        # A nonzero return can simply mean this owned process exited between
        # the decision and the call, so the outcome is re-queried rather than
        # inferred from the command's localized text.
        if not live(pid, windows=True, run=runner):
            return
        raise ProcessCommandError(
            f"taskkill did not terminate owned PID {pid}: {_command_detail(command, completed)}. "
            "A refused termination is not successful cleanup."
        )
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise ProcessCommandError(
            f"owned PID {pid} could not be killed ({type(exc).__name__}: {exc}); a missing "
            "permission is not proof of process death."
        ) from exc


def force_kill_all(pids, *, windows: bool | None = None, run=None) -> None:
    """Clean up every owned PID, then report whatever could not be cleaned."""
    failures: list[str] = []
    for pid in pids:
        try:
            force_kill(pid, windows=windows, run=run)
        except AssertionError as exc:
            failures.append(str(exc))
    if failures:
        raise ProcessCommandError(" | ".join(failures))


def wait_until(predicate, *, timeout: float = 30.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


# --------------------------------------------------------------------------
# parity and worker counts
# --------------------------------------------------------------------------

def test_spawn_workers_reproduce_the_direct_run_exactly(tmp_path):
    pack = build_pack(
        tmp_path / "pack",
        {
            "AAA-USDT-SWAP": {"specs": rising()},
            # A falling series emits no two-green event at all.
            "BBB-USDT-SWAP": {"specs": falling()},
            # A gap breaks contiguity, so the bars after it re-warm.
            "CCC-USDT-SWAP": {"specs": rising(), "drop_groups": (6,)},
        },
    )
    document = request_for(
        hypotheses=[TWO_GREEN_EVERY_BAR, TWO_GREEN_STATE_ENTRY],
        # 5m adds a second timeframe; the 300m horizon never completes inside
        # the window, so those outcomes are explicitly empty.
        timeframes=(5, TIMEFRAME),
        models=[
            {
                "id": "fh",
                "model": "fixed_horizon_path",
                "settings": {
                    "directions": ["long", "short"],
                    "commission_pct_per_side": 0.05,
                    "by_timeframe": {
                        "5": {"horizons_minutes": [5, 300], "primary_horizon_minutes": 5},
                        "30": {"horizons_minutes": [30, 300], "primary_horizon_minutes": 30},
                    },
                },
            }
        ],
    )
    direct = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "w1", workers=1
    )
    pooled = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "w2", workers=2
    )

    assert direct["counts"] == pooled["counts"] == {
        "admitted": 0, "completed": 3, "failed": 0, "not_started": 0, "planned": 3
    }
    # Specification, data-input and implementation identities must all match.
    assert direct["identities"] == pooled["identities"]
    assert semantic_summary(direct["run_root"]) == semantic_summary(pooled["run_root"])

    # Compare decoded rows, not Parquet bytes.
    first = pack_study.load_results(direct["run_root"])
    second = pack_study.load_results(pooled["run_root"])
    assert first.completed_instruments == second.completed_instruments
    for instrument_id in first.completed_instruments:
        for name in ("conditions", "episodes", "emissions", "primitives"):
            left = first.table(instrument_id, name)
            right = second.table(instrument_id, name)
            assert list(left.columns) == list(right.columns)
            assert len(left) == len(right)
            for column in left.columns:
                assert left[column].equals(right[column]), f"{instrument_id}/{name}/{column}"
        assert (
            first.jobs[instrument_id]["stats"] == second.jobs[instrument_id]["stats"]
        )
    # Physical provenance legitimately differs.
    assert direct["evidence_set_sha256"] != pooled["evidence_set_sha256"]


def test_effective_capacity_is_bounded_by_the_selected_instruments(tmp_path):
    pack = build_pack(
        tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}, "CCC-USDT-SWAP": {}}
    )
    two_targets = ["TEST_AAA-USDT-SWAP", "TEST_BBB-USDT-SWAP"]
    result = pack_study.run_study(
        request=request_for(two_targets), data_root=pack,
        output_root=tmp_path / "many", workers=5,
    )
    execution = json.loads(
        (Path(result["run_root"]) / study_evidence.PROVENANCE_FILE).read_text(encoding="utf-8")
    )["execution"]
    assert execution["requested_workers"] == 5
    assert execution["effective_workers"] == 2
    assert len(execution["worker_pids"]) == 2
    assert execution["mode"] == "spawn" and execution["start_method"] == "spawn"

    # Requesting more than one with a single target still uses a real child.
    single = pack_study.run_study(
        request=request_for(two_targets[:1]), data_root=pack,
        output_root=tmp_path / "one", workers=2,
    )
    execution = json.loads(
        (Path(single["run_root"]) / study_evidence.PROVENANCE_FILE).read_text(encoding="utf-8")
    )["execution"]
    assert execution["effective_workers"] == 1
    assert execution["mode"] == "spawn"
    assert len(execution["worker_pids"]) == 1
    assert execution["worker_pids"][0] != os.getpid()
    # The selected backend imposes no further documented worker-count limit on
    # this platform, so no arbitrary cap is invented: the only clamp is the
    # selected instrument count asserted above.
    assert not any(live(pid) for pid in execution["worker_pids"])


def test_a_fresh_child_registers_the_builtins_and_the_declared_extension(tmp_path):
    name = "ext_worker_registration"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    document = request_for(
        models=[fixed_horizon_model(TIMEFRAME, [30]), probe_model(name, mode="identify")],
        extensions=[declaration],
    )
    result = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run", workers=2
    )
    results = pack_study.load_results(result["run_root"])
    pids = set()
    for instrument_id in results.completed_instruments:
        view = results.observations(
            instrument_id, model_instance_id="probe", timeframe_minutes=TIMEFRAME,
            case_id="probe",
        )
        assert len(view)
        # Every required built-in and declared descriptor was registered in the
        # process that actually produced these rows.
        assert set(view["registered"].to_numpy()) == {3.0}
        pids.update(int(value) for value in view["worker_pid"].to_numpy())
    assert pids and os.getpid() not in pids

    direct = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "direct", workers=1
    )
    direct_results = pack_study.load_results(direct["run_root"])
    view = direct_results.observations(
        direct_results.completed_instruments[0], model_instance_id="probe",
        timeframe_minutes=TIMEFRAME, case_id="probe",
    )
    assert set(int(value) for value in view["worker_pid"].to_numpy()) == {os.getpid()}
    # The worker PID is execution provenance, so it must not enter identity.
    assert direct["identities"]["data_input_sha256"] == result["identities"]["data_input_sha256"]
    assert direct["identities"]["specification_sha256"] == result["identities"][
        "specification_sha256"
    ]


# --------------------------------------------------------------------------
# ordering and retention bounds
# --------------------------------------------------------------------------

def test_out_of_order_completion_still_yields_the_planned_canonical_evidence(
    tmp_path, monkeypatch
):
    name = "ext_worker_ordering"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    release = tmp_path / "release.marker"
    waiting = tmp_path / "waiting.marker"

    # Exactly one request, so the two runs are the same study: the first planned
    # instrument waits for a marker the fixture writes only after the second
    # one has been published.
    document = request_for(
        models=[
            fixed_horizon_model(TIMEFRAME, [30]),
            probe_model(name, mode="wait", marker=release, signal_marker=waiting,
                        only_instrument="TEST_AAA-USDT-SWAP", instance_id="delayed"),
        ],
        extensions=[declaration],
    )

    completed: list[str] = []
    real = study_runner._publish_result

    def observe(run_root, state, identifier, result):
        real(run_root, state, identifier, result)
        completed.append(identifier)
        # Release the delayed job only after the other one is published.
        if identifier == "TEST_BBB-USDT-SWAP":
            release.write_text("go", encoding="utf-8")

    monkeypatch.setattr(study_runner, "_publish_result", observe)
    assert not release.exists()
    pooled = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "pooled", workers=2
    )
    monkeypatch.undo()

    assert waiting.exists(), "the delayed job never started"
    assert completed == ["TEST_BBB-USDT-SWAP", "TEST_AAA-USDT-SWAP"]

    # The marker now exists, so the same study runs straight through directly.
    baseline = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "baseline", workers=1
    )
    assert [item["instrument_id"] for item in json.loads(
        (Path(pooled["run_root"]) / study_evidence.FAMILY_FILE).read_text(encoding="utf-8")
    )["instruments"]] == ["TEST_AAA-USDT-SWAP", "TEST_BBB-USDT-SWAP"]
    # Completion order differed from the planned order, yet the canonical
    # evidence, identities and summary are identical.
    assert pooled["identities"] == baseline["identities"]
    assert semantic_summary(pooled["run_root"]) == semantic_summary(baseline["run_root"])


def test_at_most_w_unretired_jobs_plus_one_preparation_slot_are_retained(tmp_path, monkeypatch):
    pack = build_pack(
        tmp_path / "pack",
        {f"{letter}{letter}{letter}-USDT-SWAP": {} for letter in "ABCDE"},
    )
    workers = 2
    unretired = 0
    samples: list[int] = []
    events: list[str] = []
    payload_refs: list[weakref.ref] = []
    result_refs: list[weakref.ref] = []

    returned: list[int] = []
    real_admit = study_runner._admit
    real_submit = study_workers.SpawnJobPool.submit
    real_publish = study_runner._publish_result
    real_accept = study_runner._accept

    def admit(run_root, session, entry, request, family, state):
        # Entering preparation occupies the single extra slot.
        samples.append(unretired + 1)
        events.append(f"prepare:{entry['instrument_id']}")
        return real_admit(run_root, session, entry, request, family, state)

    def submit(self, job_id, payload):
        nonlocal unretired
        real_submit(self, job_id, payload)
        unretired += 1
        samples.append(unretired)
        events.append(f"submit:{job_id}")
        payload_refs.append(weakref.ref(payload))

    def publish(run_root, state, identifier, result):
        nonlocal unretired
        real_publish(run_root, state, identifier, result)
        unretired -= 1
        events.append(f"publish:{identifier}")
        result_refs.append(weakref.ref(result))

    def accept(run_root, state, checks, message, inflight):
        # A returned result still occupies its slot until it is published.
        if message[0] == "result":
            returned.append(unretired)
            samples.append(unretired)
        real_accept(run_root, state, checks, message, inflight)

    monkeypatch.setattr(study_runner, "_admit", admit)
    monkeypatch.setattr(study_workers.SpawnJobPool, "submit", submit)
    monkeypatch.setattr(study_runner, "_publish_result", publish)
    monkeypatch.setattr(study_runner, "_accept", accept)
    result = pack_study.run_study(
        request=request_for(), data_root=pack, output_root=tmp_path / "run", workers=workers
    )
    monkeypatch.undo()

    assert result["counts"]["completed"] == 5
    # At most W unretired jobs plus one instrument being prepared.
    assert max(samples) <= workers + 1
    # The job bound is actually reached rather than trivially satisfied, and a
    # returned-but-unpublished result is counted while it waits.
    assert max(samples) == workers
    assert returned and max(returned) >= 1 and max(returned) <= workers
    # Admission stops under backpressure: never more than W submissions before
    # the first publication retires a slot.
    first_publish = next(index for index, item in enumerate(events) if item.startswith("publish:"))
    assert sum(1 for item in events[:first_publish] if item.startswith("submit:")) == workers
    assert unretired == 0

    gc.collect()
    assert all(reference() is None for reference in payload_refs), "a prepared payload was retained"
    assert all(reference() is None for reference in result_refs), "a job result was retained"


# --------------------------------------------------------------------------
# isolation
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "mode, expected",
    [
        ("network", "a network connect"),
        ("pack_read", "a pack read"),
        ("publish", "evidence publication"),
    ],
)
def test_a_worker_cannot_reach_the_network_the_pack_or_publication(tmp_path, mode, expected):
    name = f"ext_worker_guard_{mode}"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    document = request_for(
        models=[probe_model(name, mode=mode)], extensions=[declaration]
    )
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=document, data_root=pack, output_root=run_root, workers=2
        )
    assert expected in str(failure.value)
    assert failure.value.context["instrument_id"] == "TEST_AAA-USDT-SWAP"
    assert "AssertionError" in failure.value.context.get("worker_traceback", "")
    assert not study_evidence.completion_path(run_root).is_file()
    # The parent kept its own capabilities: it could still read the pack.
    with pack_data.read_session(pack) as session:
        assert session.inspect(verify=False)["state"] == "ready"


@pytest.mark.parametrize("workers", [1, 2])
def test_protected_input_arrays_cannot_be_mutated_in_either_mode(tmp_path, workers):
    name = f"ext_worker_mutate_{workers}"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    document = request_for(
        models=[probe_model(name, mode="mutate_input")], extensions=[declaration]
    )
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=document, data_root=pack, output_root=tmp_path / "run", workers=workers
        )
    assert "read-only" in str(failure.value)


def test_a_child_rejects_a_source_generation_the_coordinator_did_not_freeze(tmp_path):
    from tools.pattern_lab.study import spec as study_spec

    name = "ext_worker_source_mismatch"
    root = tmp_path / "ext"
    write_extension(root, name)
    declaration = study_spec.ExtensionDeclaration(
        module=name, source_root=str(root), helpers=()
    )
    settings = study_workers.WorkerSettings(
        extensions=(declaration,),
        required=(("model", f"{name}_model"),),
        frozen=(frozen_record(declaration, {f"{name}.py": "0" * 64}),),
    )
    with pytest.raises(PatternLabDataError) as failure:
        study_workers.initialize_worker(settings)
    assert failure.value.error_code == "source_changed"
    assert "the coordinator froze" in str(failure.value)


THREAD_PROBE = '''\
"""Preload Arrow with a non-1 configuration, then call the shared child helper."""
import json
import os
import sys

sys.path.insert(0, {root!r})

import pyarrow

pyarrow.set_cpu_count(3)
pyarrow.set_io_thread_count(2)
before = {{"cpu_count": pyarrow.cpu_count(), "io_thread_count": pyarrow.io_thread_count()}}

from tools.pattern_lab.study import workers as study_workers

observed = study_workers.configure_child_threads()
print(json.dumps({{"before": before, "observed": observed}}))
'''


def test_the_child_thread_helper_pins_a_preloaded_arrow_to_one(tmp_path):
    script = tmp_path / "thread_probe.py"
    script.write_text(
        THREAD_PROBE.format(root=str(Path(__file__).resolve().parents[2])), encoding="utf-8"
    )
    environment = dict(os.environ)
    for name in study_workers.THREAD_ENVIRONMENT:
        environment[name] = "1"
    completed = subprocess.run(
        [sys.executable, "-B", "-X", "utf8", str(script)],
        capture_output=True, text=True, timeout=SCENARIO_TIMEOUT, env=environment,
        cwd=str(Path(__file__).resolve().parents[2]),
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    assert payload["before"]["cpu_count"] == 3
    assert payload["before"]["io_thread_count"] == 2
    assert payload["observed"]["pyarrow"] == {"cpu_count": 1, "io_thread_count": 1}


def test_nondefault_caller_thread_settings_are_overridden_and_restored(tmp_path, monkeypatch):
    for variable in study_workers.THREAD_ENVIRONMENT:
        monkeypatch.setenv(variable, "7")
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    result = pack_study.run_study(
        request=request_for(), data_root=pack, output_root=tmp_path / "run", workers=2
    )
    settings = json.loads(
        (Path(result["run_root"]) / study_evidence.PROVENANCE_FILE).read_text(encoding="utf-8")
    )["execution"]["thread_settings"]
    assert settings["caller_environment"] == {
        variable: "7" for variable in study_workers.THREAD_ENVIRONMENT
    }
    observed = settings["observed_in_children"]
    assert len(observed) == 2
    for child in observed:
        assert child["environment"] == {
            variable: "1" for variable in study_workers.THREAD_ENVIRONMENT
        }
        assert child["pyarrow"] == {"cpu_count": 1, "io_thread_count": 1}
    # The caller regains its own mapping after a successful teardown.
    assert all(os.environ[variable] == "7" for variable in study_workers.THREAD_ENVIRONMENT)

    # ... and after a failure too.
    name = "ext_worker_threads_failure"
    declaration = write_extension(tmp_path / "ext", name)
    with pytest.raises(PatternLabStudyError):
        pack_study.run_study(
            request=request_for(models=[probe_model(name, mode="raise")],
                                extensions=[declaration]),
            data_root=pack, output_root=tmp_path / "failed", workers=2,
        )
    assert all(os.environ[variable] == "7" for variable in study_workers.THREAD_ENVIRONMENT)


def test_a_printing_extension_leaves_the_cli_stdout_parseable(tmp_path):
    name = "ext_worker_printing"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    document = request_for(
        models=[probe_model(name, mode="print")], extensions=[declaration]
    )
    spec_path = tmp_path / "study.json"
    spec_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable, "-B", "-X", "utf8", "-m", "tools.pattern_lab", "study",
            "--spec", str(spec_path), "--data-root", str(pack),
            "--output-root", str(tmp_path / "run"), "--workers", "2",
        ],
        capture_output=True, text=True, timeout=SCENARIO_TIMEOUT,
        cwd=str(Path(__file__).resolve().parents[2]),
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["status"] == "completed"
    assert payload["counts"]["completed"] == 2
    # The extension's noise went to stderr, where diagnostics belong.
    assert "noise on stdout from a trusted extension" in completed.stderr
    assert "noise on stdout" not in completed.stdout


# --------------------------------------------------------------------------
# failures
# --------------------------------------------------------------------------

def test_a_worker_initialization_failure_is_a_run_level_failure(tmp_path):
    name = "ext_worker_init_failure"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name, FAILING_INITIALIZER)
    document = request_for(
        # This module's model takes no settings; it fails only in a worker.
        models=[{"id": "probe", "model": f"{name}_model", "settings": {}}],
        extensions=[declaration],
    )
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=document, data_root=pack, output_root=run_root, workers=2
        )
    assert "worker initialization failed" in str(failure.value)
    # No instrument is invented, and nothing was dispatched.
    assert failure.value.context["instrument_id"] is None
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "failed"
    assert status["counts"] == {
        "admitted": 0, "completed": 0, "failed": 0, "not_started": 2, "planned": 2
    }
    assert not study_evidence.completion_path(run_root).is_file()


def test_a_publication_failure_stops_dispatch_and_keeps_finished_work(tmp_path):
    name = "ext_worker_numerical_failure"
    pack = build_pack(
        tmp_path / "pack",
        {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}, "CCC-USDT-SWAP": {}, "DDD-USDT-SWAP": {}},
    )
    declaration = write_extension(tmp_path / "ext", name)
    # Only the second planned instrument fails, so the first can finish first.
    document = request_for(
        models=[probe_model(name, mode="ok")], extensions=[declaration]
    )
    run_root = tmp_path / "run"
    real = study_runner._publish_result

    def fail_second(run_root_arg, state, identifier, result):
        if identifier == "TEST_BBB-USDT-SWAP":
            raise ValueError("synthetic publication failure")
        real(run_root_arg, state, identifier, result)

    study_runner._publish_result = fail_second
    try:
        with pytest.raises(PatternLabStudyError) as failure:
            pack_study.run_study(
                request=document, data_root=pack, output_root=run_root, workers=2
            )
    finally:
        study_runner._publish_result = real

    assert failure.value.context["instrument_id"] == "TEST_BBB-USDT-SWAP"
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "failed"
    states = {item["instrument_id"]: item["state"] for item in status["instruments"]}
    assert states["TEST_BBB-USDT-SWAP"] == "failed"
    assert not study_evidence.completion_path(run_root).is_file()
    # Counts, per-job state and the terminal reason agree, and every other
    # dispatched job carries the distinct aborted reason.
    counts = status["counts"]
    assert counts["planned"] == 4
    assert counts["completed"] + counts["failed"] + counts["not_started"] == 4
    for item in status["instruments"]:
        if item["state"] == "failed" and item["instrument_id"] != "TEST_BBB-USDT-SWAP":
            assert item["error"] == study_runner.CANCELLED_REASON
    partial = pack_study.load_results(run_root, allow_partial=True)
    assert partial.complete is False
    assert set(partial.completed_instruments) == {
        item["instrument_id"] for item in status["instruments"] if item["state"] == "completed"
    }


def test_a_worker_job_exception_names_the_instrument_and_keeps_the_child_traceback(tmp_path):
    name = "ext_worker_raise"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=request_for(models=[probe_model(name, mode="raise")],
                                extensions=[declaration]),
            data_root=pack, output_root=run_root, workers=2,
        )
    assert failure.value.context["instrument_id"] == "TEST_AAA-USDT-SWAP"
    assert "synthetic numerical failure inside the job" in str(failure.value)
    assert "ValueError" in failure.value.context["worker_traceback"]
    assert study_evidence.read_status(run_root)["terminal_status"] == "failed"


def test_an_abrupt_child_exit_is_an_actionable_failure_not_an_endless_wait(tmp_path):
    name = "ext_worker_abrupt_exit"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    died = tmp_path / "died.marker"
    run_root = tmp_path / "run"
    started = time.monotonic()
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=request_for(
                models=[probe_model(name, mode="abrupt_exit", signal_marker=died)],
                extensions=[declaration],
            ),
            data_root=pack, output_root=run_root, workers=2,
        )
    assert time.monotonic() - started < SCENARIO_TIMEOUT
    assert died.exists()
    assert failure.value.error_code == "worker_lost"
    assert failure.value.context["instrument_id"] == "TEST_AAA-USDT-SWAP"
    assert "exited unexpectedly" in str(failure.value)
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "failed"
    assert not study_evidence.completion_path(run_root).is_file()


def test_an_unserializable_payload_fails_with_its_job_named(tmp_path):
    settings = study_workers.WorkerSettings(extensions=(), required=(), frozen=())
    pool = study_workers.SpawnJobPool(1, settings)
    # No worker is started: serialization happens in the coordinator, so the
    # failure is synchronous and attributable to this job.
    with pytest.raises(study_workers.WorkerJobError) as failure:
        pool.submit("TEST_AAA-USDT-SWAP", lambda: None)
    assert "TEST_AAA-USDT-SWAP" in str(failure.value)
    assert "could not be serialized" in str(failure.value)
    assert failure.value.error_code == "job_failed"


def test_an_interrupt_during_publication_stops_dispatch_and_cleans_up(tmp_path):
    pack = build_pack(
        tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}, "CCC-USDT-SWAP": {}}
    )
    run_root = tmp_path / "run"
    real = study_runner._publish_result
    seen: list[str] = []
    pids: list[int] = []
    real_start = study_workers.SpawnJobPool.start

    def record_start(self):
        real_start(self)
        pids.extend(self.worker_pids)

    def interrupt(run_root_arg, state, identifier, result):
        seen.append(identifier)
        raise KeyboardInterrupt

    study_runner._publish_result = interrupt
    study_workers.SpawnJobPool.start = record_start
    try:
        with pytest.raises(KeyboardInterrupt):
            pack_study.run_study(
                request=request_for(), data_root=pack, output_root=run_root, workers=2
            )
    finally:
        study_runner._publish_result = real
        study_workers.SpawnJobPool.start = real_start

    assert len(seen) == 1
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "interrupted"
    assert not study_evidence.completion_path(run_root).is_file()
    assert status["counts"]["completed"] == 0
    assert pids and not any(live(pid) for pid in pids)


# --------------------------------------------------------------------------
# cancellation, oversized transfers and child lifetime
# --------------------------------------------------------------------------

def test_a_blocked_worker_and_a_failure_shut_down_within_the_deadline(tmp_path):
    name = "ext_worker_blocked"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    never = tmp_path / "never-released.marker"
    waiting = tmp_path / "waiting.marker"
    run_root = tmp_path / "run"

    real = study_runner._publish_result
    pids: list[int] = []
    real_start = study_workers.SpawnJobPool.start

    def record_start(self):
        real_start(self)
        pids.extend(self.worker_pids)

    def fail_on_first(run_root_arg, state, identifier, result):
        raise ValueError("synthetic failure while another worker is blocked")

    study_runner._publish_result = fail_on_first
    study_workers.SpawnJobPool.start = record_start
    started = time.monotonic()
    try:
        with pytest.raises(PatternLabStudyError):
            pack_study.run_study(
                request=request_for(
                    models=[
                        fixed_horizon_model(TIMEFRAME, [30]),
                        # The first planned instrument blocks forever; the
                        # second finishes and then fails in publication.
                        probe_model(name, mode="wait", marker=never, signal_marker=waiting,
                                    only_instrument="TEST_AAA-USDT-SWAP",
                                    instance_id="blocked"),
                    ],
                    extensions=[declaration],
                ),
                data_root=pack, output_root=run_root, workers=2,
            )
    finally:
        study_runner._publish_result = real
        study_workers.SpawnJobPool.start = real_start
    elapsed = time.monotonic() - started

    assert not never.exists(), "the blocked job must never have been released"
    # Graceful cleanup is attempted, then forced, within a bounded deadline.
    assert elapsed < SCENARIO_TIMEOUT
    assert pids
    assert wait_until(lambda: not any(live(pid) for pid in pids), timeout=30.0)
    assert study_evidence.read_status(run_root)["terminal_status"] == "failed"
    assert not study_evidence.completion_path(run_root).is_file()


def test_the_child_ignores_sigint_so_the_coordinator_owns_ctrl_c(tmp_path, monkeypatch):
    name = "ext_worker_sigint"
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    declaration = write_extension(tmp_path / "ext", name)
    release = tmp_path / "release.marker"
    waiting = tmp_path / "waiting.marker"
    observed: dict = {}
    real_take = study_workers.SpawnJobPool.take

    def interfere(self, **kwargs):
        # The coordinator is waiting here while the worker sits in its
        # handshake, so the signal is delivered at a known moment.
        if not observed:
            assert wait_until(waiting.exists, timeout=120.0), "the worker never handshook"
            pid = self.worker_pids[0]
            if os.name == "nt":  # pragma: no cover - platform dependent
                # Windows has no supported way to deliver Ctrl+C to one child
                # that does not share a console group, so only the installed
                # policy is checked there; the job itself asserts it.
                observed["alive_after_sigint"] = None
            else:
                os.kill(pid, signal.SIGINT)
                time.sleep(0.5)
                observed["alive_after_sigint"] = live(pid)
            release.write_text("go", encoding="utf-8")
        return real_take(self, **kwargs)

    monkeypatch.setattr(study_workers.SpawnJobPool, "take", interfere)
    result = pack_study.run_study(
        request=request_for(
            models=[probe_model(name, mode="sigint_probe", marker=release,
                                signal_marker=waiting)],
            extensions=[declaration],
        ),
        data_root=pack, output_root=tmp_path / "run", workers=2,
    )
    monkeypatch.undo()
    # The job itself asserted that SIGINT was SIG_IGN before any extension ran,
    # so reaching a completed run proves the policy on either platform.
    assert result["counts"]["completed"] == 1
    if os.name != "nt":
        # On POSIX a real signal was delivered and the worker survived it, so
        # the coordinator alone owns handled Ctrl+C.
        assert observed["alive_after_sigint"] is True


def test_an_oversized_transfer_and_a_lost_child_never_deadlock(tmp_path):
    """One isolated scenario over one pool: large transfers, then a killed child.

    What is deterministic here: the payload and the returned tables both exceed
    a pipe buffer, the second job is killed only after the child has signalled
    that it started it, no result for that job ever arrives, the pool reports it
    as lost within a bounded time, and teardown finishes within the cleanup
    deadline with no owned child left alive. What is not deterministic, and is
    not asserted, is the exact instruction the child died on.
    """
    name = "ext_worker_oversized"
    # Enough bars that both the input payload and the returned tables exceed a
    # pipe buffer, so the transfer really is chunked.
    groups = 2000
    pack = build_pack(
        tmp_path / "pack", {"AAA-USDT-SWAP": {"specs": rising(groups)}}, groups=groups
    )
    declaration = write_extension(tmp_path / "ext", name)
    never = tmp_path / "never-released.marker"
    started = tmp_path / "started.marker"
    horizons = fixed_horizon_model(TIMEFRAME, [30, 60, 90, 120])

    def document(models):
        return request_for(
            ["TEST_AAA-USDT-SWAP"], models=models, extensions=[declaration],
            end_group=groups - 4, coverage_groups=groups + 8,
        )

    plain = pack_study.normalize_request(document([horizons]), source="test", base=None)
    blocking = pack_study.normalize_request(
        document([
            horizons,
            probe_model(name, mode="wait", marker=never, signal_marker=started,
                        instance_id="blocked"),
        ]),
        source="test", base=None,
    )
    loaded = study_extensions.load_extensions(blocking.extensions)
    settings = study_workers.WorkerSettings(
        extensions=tuple(blocking.extensions),
        required=(
            ("hypothesis", "two_green_rising_quote_volume"),
            ("model", "fixed_horizon_path"),
            ("model", f"{name}_model"),
        ),
        frozen=tuple(loaded),
    )

    with pack_data.read_session(pack) as session:
        entry = next(
            item for item in session.inspect(verify=False)["instruments"]
            if item["instrument_id"] == "TEST_AAA-USDT-SWAP"
        )
        base = session.load_slice(
            "TEST_AAA-USDT-SWAP",
            start=study_runner.format_epoch_ms(plain.study_start_ms),
            end=study_runner.format_epoch_ms(plain.study_end_ms),
            warmup_start=study_runner.format_epoch_ms(plain.warmup_start_ms),
            timeframe_minutes=5,
        )
        prepared, _fingerprints = study_runner._prepare_timeframes(entry, base, plain)
    assert prepared[0].values.nbytes > 65536, "the payload must exceed a pipe buffer"

    with study_workers.single_threaded_children():
        pool = study_workers.SpawnJobPool(1, settings)
        pool.start()
        pids = list(pool.worker_pids)
        try:
            pool.submit("TEST_AAA-USDT-SWAP", study_runner._job_payload(entry, plain, prepared))
            kind, job_id, result = pool.take(timeout=SCENARIO_TIMEOUT)
            assert kind == "result" and job_id == "TEST_AAA-USDT-SWAP"
            assert len(result.tables["primitives"]) > 4000

            pool.submit(
                "TEST_AAA-USDT-SWAP", study_runner._job_payload(entry, blocking, prepared)
            )
            assert wait_until(started.exists, timeout=120.0), "the worker never started the job"
            force_kill_all(pids)
            clock = time.monotonic()
            with pytest.raises(study_workers.WorkerLostError) as failure:
                pool.take(timeout=SCENARIO_TIMEOUT)
            assert time.monotonic() - clock < 60.0
            assert failure.value.orphaned == ("TEST_AAA-USDT-SWAP",)
            assert failure.value.error_code == "worker_lost"
        finally:
            clock = time.monotonic()
            report = pool.shutdown(abort=True)
            # A broken channel is abandoned rather than drained indefinitely.
            assert time.monotonic() - clock < study_workers.CLEANUP_DEADLINE_SECONDS + 10.0
            assert report["abort"] is True
    assert not never.exists()
    assert wait_until(lambda: not any(live(pid) for pid in pids), timeout=30.0)
    assert pool.live_workers() == []


# --------------------------------------------------------------------------
# coordinator death
# --------------------------------------------------------------------------

PARENT_DEATH_SCRIPT = '''\
"""A fixture coordinator: hold the pack session, keep one spawn child alive."""
import json
import multiprocessing
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, {root!r})

from tools.pattern_lab import data as pack_data


def idle(ready_path):
    Path(ready_path).write_text(str(os.getpid()), encoding="utf-8")
    while True:
        time.sleep(0.2)


def main():
    pack, marker, child_ready = sys.argv[1], sys.argv[2], sys.argv[3]
    context = multiprocessing.get_context("spawn")
    with pack_data.read_session(Path(pack)) as session:
        session.inspect(verify=False)
        child = context.Process(target=idle, args=(child_ready,), daemon=False)
        child.start()
        Path(marker).write_text(
            json.dumps({{"coordinator": os.getpid(), "child": child.pid}}), encoding="utf-8"
        )
        while True:
            time.sleep(0.2)


if __name__ == "__main__":
    main()
'''


def test_killing_the_coordinator_releases_the_pack_and_publishes_nothing(tmp_path):
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    output = tmp_path / "run"
    script = tmp_path / "fixture_coordinator.py"
    script.write_text(
        PARENT_DEATH_SCRIPT.format(root=str(Path(__file__).resolve().parents[2])),
        encoding="utf-8",
    )
    marker = tmp_path / "coordinator.json"
    child_ready = tmp_path / "child.pid"
    process = subprocess.Popen(
        [sys.executable, "-B", "-X", "utf8", str(script), str(pack), str(marker),
         str(child_ready)],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    owned: list[int] = [process.pid]
    try:
        assert wait_until(lambda: marker.is_file() and child_ready.is_file(), timeout=120.0), (
            "the fixture coordinator never reported its child"
        )
        record = json.loads(marker.read_text(encoding="utf-8"))
        owned.append(int(record["child"]))
        assert live(record["child"])

        # While it lives, the pack guard is genuinely held.
        from tools.pattern_lab import PatternLabBusyError

        with pytest.raises(PatternLabBusyError):
            with pack_data.read_session(pack):
                pass

        process.kill()
        process.wait(timeout=60.0)
        # The OS releases the guard on process exit; no lease or watchdog exists.
        assert wait_until(lambda: _can_acquire(pack), timeout=60.0)
        # The child performed no pack read, no publication and no completion.
        assert not output.exists()
        assert not any(pack.glob("**/*.tmp*"))
    finally:
        # Every owned PID is cleaned up even if one cleanup operation fails.
        force_kill_all(owned)
        try:
            process.wait(timeout=30.0)
        except subprocess.TimeoutExpired:  # pragma: no cover - already killed
            pass
    assert wait_until(lambda: not any(live(pid) for pid in owned), timeout=30.0)


def _can_acquire(pack: Path) -> bool:
    try:
        with pack_data.read_session(pack):
            return True
    except Exception:
        return False


# --------------------------------------------------------------------------
# declared helper source generations in a fresh child (T04-1 R3)
# --------------------------------------------------------------------------

HELPER_EXTENSION = '''\
"""A declared extension whose model value comes from a declared helper file.

A child that reaches module execution leaves an import marker behind, so a
rejection that must happen *before* the changed extension runs is provable.
"""

import multiprocessing
import os

import numpy as np

from tools.pattern_lab.study import contracts
from tools.pattern_lab.study.contracts import (
    ModelCase,
    ModelDescriptor,
    ModelEvidence,
    OutcomeSpec,
)

import __HELPER__ as helper

if multiprocessing.parent_process() is not None:
    open(os.path.join("__MARKERS__", "child-%d.imported" % os.getpid()), "w").close()
    __IMPORT_TIME_EDIT__


def _validate(settings, timeframes):
    contracts.closed_keys(settings, (), "settings")
    return {}


def _cases(settings, timeframe):
    return (
        ModelCase(
            case_id="probe",
            timeframe_minutes=int(timeframe),
            parameters={},
            outcomes=(OutcomeSpec("helper_value", "count", "the declared helper's value"),),
            primary=True,
        ),
    )


def _evaluate(series, settings, anchors):
    count = int(anchors.rows.size)
    return ModelEvidence(
        kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
        rows={
            "case_id": np.full(count, "probe", dtype=object),
            "anchor_open_ms": anchors.open_ms.astype(np.int64),
            "helper_value": np.full(count, float(helper.VALUE), dtype=np.float64),
            "helper_value__reason": np.full(count, "available", dtype=object),
        },
    )


def register(context):
    context.register_model(
        ModelDescriptor(
            model_id="__MODEL_ID__",
            version="1",
            validate_settings=_validate,
            resolve_cases=_cases,
            evaluate=_evaluate,
        )
    )
'''

HELPER_VALUE = 3.0


def write_helper_extension(root: Path, name: str, *, markers: Path,
                           import_time_edit: bool = False) -> dict:
    """Write a declared module plus the local helper it imports at import time."""
    root.mkdir(parents=True, exist_ok=True)
    markers.mkdir(parents=True, exist_ok=True)
    helper_name = f"{name}_helper"
    helper_path = root / f"{helper_name}.py"
    helper_path.write_text(f"VALUE = {HELPER_VALUE}\n", encoding="utf-8")
    edit = (
        'open(__HELPER_PATH__, "w", encoding="utf-8").write("VALUE = 42.0\\n")'
        if import_time_edit
        else "pass"
    )
    body = (
        HELPER_EXTENSION
        .replace("__IMPORT_TIME_EDIT__", edit)
        .replace("__HELPER_PATH__", repr(str(helper_path)))
        .replace("__HELPER__", helper_name)
        .replace('"__MARKERS__"', repr(str(markers)))
        .replace("__MODEL_ID__", f"{name}_model")
    )
    (root / f"{name}.py").write_text(body, encoding="utf-8")
    return {"module": name, "source_root": str(root), "helpers": [f"{helper_name}.py"]}


def helper_request(declaration: dict, name: str, instrument_ids):
    return request_for(
        instrument_ids,
        models=[{"id": "probe", "model": f"{name}_model", "settings": {}}],
        extensions=[declaration],
    )


def edit_before_children_start(monkeypatch, path: Path, text: str) -> list[int]:
    """Change one declared source file between the freeze and child startup.

    Returns the list the started workers' PIDs are recorded in, so the caller
    can prove the pool really was cleaned up.
    """
    real_start = study_workers.SpawnJobPool.start
    pids: list[int] = []

    def start(self):
        path.write_text(text, encoding="utf-8")
        try:
            real_start(self)
        finally:
            pids.extend(int(worker.pid) for worker in self._workers if worker.pid)

    monkeypatch.setattr(study_workers.SpawnJobPool, "start", start)
    return pids


def test_a_fresh_child_accepts_unchanged_declared_helper_source(tmp_path):
    name = "ext_worker_helper_ok"
    root = tmp_path / "ext"
    markers = tmp_path / "imports"
    declaration = write_helper_extension(root, name, markers=markers)
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    run_root = tmp_path / "run"
    result = pack_study.run_study(
        request=helper_request(declaration, name, ["TEST_AAA-USDT-SWAP", "TEST_BBB-USDT-SWAP"]),
        data_root=pack, output_root=run_root, workers=2,
    )
    assert result["status"] == "completed"
    # The declared helper really was imported and used inside fresh children.
    assert sorted(path.name for path in markers.iterdir())
    results = pack_study.load_results(run_root)
    frame = results.observations(
        "TEST_AAA-USDT-SWAP", model_instance_id="probe",
        timeframe_minutes=TIMEFRAME, case_id="probe",
    )
    assert set(frame["helper_value"].tolist()) == {HELPER_VALUE}


@pytest.mark.parametrize("changed", ["helper", "module"])
def test_a_source_change_after_the_freeze_fails_every_child(tmp_path, monkeypatch, changed):
    name = f"ext_worker_frozen_{changed}"
    root = tmp_path / "ext"
    markers = tmp_path / "imports"
    declaration = write_helper_extension(root, name, markers=markers)
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}})
    run_root = tmp_path / "run"
    if changed == "helper":
        # The main module is untouched: only a declared helper changes.
        target, text = root / f"{name}_helper.py", "VALUE = 99.0\n"
    else:
        target = root / f"{name}.py"
        text = (root / f"{name}.py").read_text(encoding="utf-8") + "\n# changed\n"
    pids = edit_before_children_start(monkeypatch, target, text)

    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=helper_request(declaration, name, ["TEST_AAA-USDT-SWAP", "TEST_BBB-USDT-SWAP"]),
            data_root=pack, output_root=run_root, workers=2,
        )
    monkeypatch.undo()

    assert failure.value.error_code == "source_changed"
    assert "the coordinator froze" in str(failure.value)
    # Rejection happened before any child executed the changed generation.
    assert list(markers.iterdir()) == []
    # An initialization failure is run-level: no instrument is invented.
    assert failure.value.context["instrument_id"] is None
    status = study_evidence.read_status(run_root)
    assert status["counts"] == {
        "admitted": 0, "completed": 0, "failed": 0, "not_started": 2, "planned": 2
    }
    assert not study_evidence.completion_path(run_root).is_file()
    assert not (run_root / study_evidence.JOBS_DIR).exists()
    # The bounded pool cleanup really ran: no owned child survives.
    assert pids and wait_until(lambda: not any(live(pid) for pid in pids), timeout=30.0)


def test_an_import_time_source_change_is_rejected_after_import(tmp_path):
    name = "ext_worker_import_time_edit"
    root = tmp_path / "ext"
    markers = tmp_path / "imports"
    declaration = write_helper_extension(root, name, markers=markers, import_time_edit=True)
    # One instrument, so exactly one child rewrites its own declared helper.
    pack = build_pack(tmp_path / "pack", {"AAA-USDT-SWAP": {}})
    run_root = tmp_path / "run"
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=helper_request(declaration, name, ["TEST_AAA-USDT-SWAP"]),
            data_root=pack, output_root=run_root, workers=2,
        )
    assert failure.value.error_code == "source_changed"
    assert "after import" in str(failure.value)
    # The module did run here: the post-import recheck is what caught it.
    assert list(markers.iterdir())
    assert not study_evidence.completion_path(run_root).is_file()


def test_a_child_requires_a_frozen_record_for_every_declared_file(tmp_path):
    from tools.pattern_lab.study import spec as study_spec

    name = "ext_worker_uncovered_helper"
    root = tmp_path / "ext"
    markers = tmp_path / "imports"
    write_helper_extension(root, name, markers=markers)
    declaration = study_spec.ExtensionDeclaration(
        module=name, source_root=str(root), helpers=(f"{name}_helper.py",)
    )
    # A record that covers the main module only cannot admit the declaration.
    settings = study_workers.WorkerSettings(
        extensions=(declaration,),
        required=(("model", f"{name}_model"),),
        frozen=(
            frozen_record(
                declaration,
                {f"{name}.py": study_extensions.file_digest(root / f"{name}.py")},
            ),
        ),
    )
    with pytest.raises(PatternLabDataError) as failure:
        study_workers.initialize_worker(settings)
    assert failure.value.error_code == "unverified_source"
    assert "exactly the same files" in str(failure.value)


# --------------------------------------------------------------------------
# result-pump liveness (T04-1 R5)
# --------------------------------------------------------------------------

class BrokenInbox(queue_module.Queue):
    """An in-process inbox whose ``put`` fails, so the result pump dies."""

    def put(self, item, *args, **kwargs):
        raise OSError("synthetic inbox failure inside the result pump")


def test_a_dead_result_pump_fails_promptly_instead_of_waiting_for_the_deadline(tmp_path):
    settings = study_workers.WorkerSettings(extensions=(), required=(), frozen=())
    with study_workers.single_threaded_children():
        pool = study_workers.SpawnJobPool(1, settings)
        pool.start()
        pids = list(pool.worker_pids)
        try:
            # The worker's next message cannot be delivered, so the pump exits
            # while the worker itself stays alive.
            pool._inbox = BrokenInbox()
            pool.submit("TEST_AAA-USDT-SWAP", None)
            clock = time.monotonic()
            with pytest.raises(study_workers.WorkerTransportError) as failure:
                pool.take(timeout=study_workers.RESULT_DEADLINE_SECONDS)
            waited = time.monotonic() - clock
            assert waited < 60.0, "the transport failure must not wait for the result deadline"
            assert failure.value.error_code == "transport_failed"
            assert "OSError: synthetic inbox failure" in str(failure.value)
            # A live-but-blocked pump is a different case: this worker is alive.
            assert pool.live_workers() == pids
            # The nonblocking progress path reports it too, so admission stops.
            with pytest.raises(study_workers.WorkerTransportError):
                pool.poll()
        finally:
            pool.shutdown(abort=True)
    assert pool.live_workers() == []
    assert wait_until(lambda: not any(live(pid) for pid in pids), timeout=30.0)


def test_a_result_pump_that_dies_during_startup_fails_promptly(tmp_path):
    settings = study_workers.WorkerSettings(extensions=(), required=(), frozen=())
    with study_workers.single_threaded_children():
        pool = study_workers.SpawnJobPool(1, settings)
        pool._inbox = BrokenInbox()
        clock = time.monotonic()
        try:
            with pytest.raises(study_workers.WorkerTransportError) as failure:
                pool.start()
            assert time.monotonic() - clock < 60.0
            assert failure.value.error_code == "transport_failed"
            pids = pool.live_workers()
        finally:
            pool.shutdown(abort=True)
    assert pool.live_workers() == []
    assert wait_until(lambda: not any(live(pid) for pid in pids), timeout=30.0)


def test_lost_worker_precedes_dead_pump_in_both_receive_paths():
    settings = study_workers.WorkerSettings(extensions=(), required=(), frozen=())
    with study_workers.single_threaded_children():
        pool = study_workers.SpawnJobPool(1, settings)
        try:
            pool.start()
            worker = pool._workers[0]
            identifier = "TEST_AAA-USDT-SWAP"
            # Finish the pump first, while the idle child is still alive: a
            # child killed with the shared result queue's write lock held can
            # leave the sentinel undeliverable, so pump termination would not be
            # guaranteed the other way round.
            pool._results.put(None)
            pool._pump.join(timeout=10)
            assert not pool._pump.is_alive()
            # Only then the known pending job, its transport 'started' message
            # and the child's death.
            pool._pending.add(identifier)
            pool._inbox.put(("started", identifier, worker.pid))
            worker.terminate()
            worker.join(timeout=10)
            assert not worker.is_alive()
            for receive in (pool.poll, lambda: pool.take(timeout=1)):
                with pytest.raises(study_workers.WorkerLostError) as failure:
                    receive()
                assert failure.value.error_code == "worker_lost"
                assert failure.value.orphaned == (identifier,)
        finally:
            pool.shutdown(abort=True)
    assert pool.live_workers() == []


def test_normal_teardown_is_not_reported_as_a_transport_failure(tmp_path):
    settings = study_workers.WorkerSettings(extensions=(), required=(), frozen=())
    with study_workers.single_threaded_children():
        pool = study_workers.SpawnJobPool(1, settings)
        pool.start()
        assert pool.poll() is None
        report = pool.shutdown(abort=False)
    assert report["abort"] is False
    assert pool.live_workers() == []
    # The pump ended on its own sentinel, and a closed pool is not a failure.
    assert wait_until(lambda: pool._pump_exit == "the teardown sentinel was received")
    assert pool.poll() is None


def test_a_transport_failure_stops_a_run_without_naming_an_instrument(tmp_path, monkeypatch):
    pack = build_pack(
        tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}, "CCC-USDT-SWAP": {}}
    )
    run_root = tmp_path / "run"
    real_start = study_workers.SpawnJobPool.start
    pids: list[int] = []

    def start_then_break(self):
        real_start(self)
        pids.extend(self.worker_pids)
        # Every child initialized; the first result now kills the pump.
        self._inbox = BrokenInbox()

    monkeypatch.setattr(study_workers.SpawnJobPool, "start", start_then_break)
    clock = time.monotonic()
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=request_for(), data_root=pack, output_root=run_root, workers=2
        )
    monkeypatch.undo()

    assert time.monotonic() - clock < SCENARIO_TIMEOUT
    assert failure.value.error_code == "transport_failed"
    # A dead transport belongs to the run, not to one instrument.
    assert failure.value.context["instrument_id"] is None
    assert failure.value.context["phase"] == "execute"
    status = study_evidence.read_status(run_root)
    assert status["terminal_status"] == "failed"
    assert status["counts"]["completed"] == 0
    aborted = [item for item in status["instruments"] if item["state"] == "failed"]
    assert aborted, "dispatched work without an accepted result must be aborted"
    assert all(item["error"].startswith("aborted:") for item in aborted)
    assert not study_evidence.completion_path(run_root).is_file()
    assert wait_until(lambda: not any(live(pid) for pid in pids), timeout=30.0)


# --------------------------------------------------------------------------
# admission failure inside the pool (T04-1 R1)
# --------------------------------------------------------------------------

def test_an_ordinary_preparation_failure_names_its_instrument_in_the_pool(tmp_path, monkeypatch):
    pack = build_pack(
        tmp_path / "pack", {"AAA-USDT-SWAP": {}, "BBB-USDT-SWAP": {}, "CCC-USDT-SWAP": {}}
    )
    run_root = tmp_path / "run"
    real = study_runner._prepare_timeframes

    def prepare(entry, *args, **kwargs):
        if entry["symbol"] == "BBB":
            raise RuntimeError("synthetic preparation failure")
        return real(entry, *args, **kwargs)

    monkeypatch.setattr(study_runner, "_prepare_timeframes", prepare)
    with pytest.raises(PatternLabStudyError) as failure:
        pack_study.run_study(
            request=request_for(), data_root=pack, output_root=run_root, workers=2
        )
    monkeypatch.undo()

    assert failure.value.context["instrument_id"] == "TEST_BBB-USDT-SWAP"
    assert failure.value.context["phase"] == "prepare"
    states = {
        item["instrument_id"]: item
        for item in study_evidence.read_status(run_root)["instruments"]
    }
    assert states["TEST_BBB-USDT-SWAP"]["state"] == "failed"
    assert "RuntimeError: synthetic preparation failure" in states["TEST_BBB-USDT-SWAP"]["error"]
    assert states["TEST_CCC-USDT-SWAP"]["state"] == "not_started"
    counts = study_evidence.read_status(run_root)["counts"]
    assert counts["planned"] == 3 and counts["admitted"] == 0
    assert dict(pack_study.load_results(run_root, allow_partial=True).counts) == counts
    assert not study_evidence.completion_path(run_root).is_file()


# --------------------------------------------------------------------------
# the Windows process helpers' command decisions
# --------------------------------------------------------------------------
# These run the helpers' decision logic with injected command outcomes.  They
# exercise the decisions on any host; they do not certify actual Windows
# execution, which needs the focused Windows commands.

def command_result(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout,
                                       stderr=stderr)


class FakeProcessTool:
    """Answer ``tasklist``/``taskkill`` with queued outcomes, recording calls."""

    def __init__(self, **responses):
        self.responses = {name: list(items) for name, items in responses.items()}
        self.calls: list[list[str]] = []

    def __call__(self, command, **kwargs):
        self.calls.append(list(command))
        queued = self.responses.get(command[0])
        if not queued:
            raise AssertionError(f"unexpected {command[0]} invocation")
        outcome = queued.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def test_the_windows_live_probe_separates_absence_from_a_failed_query():
    running = FakeProcessTool(tasklist=[command_result(stdout="python.exe   4242 Console  1  9 K")])
    assert live(4242, windows=True, run=running) is True
    assert running.calls[0][0] == "tasklist"

    # A successful query that matched nothing is an answer; the localized
    # "no tasks" text is never parsed.
    absent = FakeProcessTool(tasklist=[command_result(stdout="INFO: keine Tasks\n")])
    assert live(4242, windows=True, run=absent) is False

    denied = FakeProcessTool(
        tasklist=[command_result(returncode=1, stderr="ERROR: Access is denied.")]
    )
    with pytest.raises(AssertionError) as failure:
        live(4242, windows=True, run=denied)
    assert "returned 1" in str(failure.value)
    assert "Access is denied" in str(failure.value)
    assert "not proof" in str(failure.value)

    unavailable = FakeProcessTool(tasklist=[FileNotFoundError("tasklist")])
    with pytest.raises(AssertionError) as failure:
        live(4242, windows=True, run=unavailable)
    assert "could not be invoked" in str(failure.value)


def test_the_windows_kill_helper_reports_a_failed_termination():
    ok = FakeProcessTool(taskkill=[command_result()])
    force_kill(4242, windows=True, run=ok)
    assert [call[0] for call in ok.calls] == ["taskkill"]

    # A nonzero return for a PID that has already exited is not a failure.
    already_gone = FakeProcessTool(
        taskkill=[command_result(returncode=128, stderr="ERROR: not found")],
        tasklist=[command_result(stdout="INFO: no tasks")],
    )
    force_kill(4242, windows=True, run=already_gone)
    assert [call[0] for call in already_gone.calls] == ["taskkill", "tasklist"]

    refused = FakeProcessTool(
        taskkill=[command_result(returncode=1, stderr="ERROR: Access is denied.")],
        tasklist=[command_result(stdout="python.exe   4242 Console")],
    )
    with pytest.raises(AssertionError) as failure:
        force_kill(4242, windows=True, run=refused)
    assert "did not terminate owned PID 4242" in str(failure.value)
    assert "Access is denied" in str(failure.value)


def test_every_owned_process_is_cleaned_up_even_when_one_operation_fails():
    tool = FakeProcessTool(
        taskkill=[
            command_result(returncode=1, stderr="ERROR: Access is denied."),
            command_result(),
        ],
        tasklist=[command_result(stdout="python.exe   11 Console")],
    )
    with pytest.raises(AssertionError) as failure:
        force_kill_all([11, 22], windows=True, run=tool)
    assert "did not terminate owned PID 11" in str(failure.value)
    # The second owned PID was still cleaned up.
    assert [call for call in tool.calls if call[0] == "taskkill"][1][2] == "22"
