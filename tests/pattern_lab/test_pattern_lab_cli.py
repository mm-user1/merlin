"""CLI contracts, import isolation and missing-dependency behavior."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

import pytest

from tools.pattern_lab import collect as pack_collect
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import exchange_data
from tools.pattern_lab import manifest as pack_manifest
from tools.pattern_lab import pack_lock
from tools.pattern_lab import update_transaction
from tools.pattern_lab.__main__ import (
    EXIT_BUSY,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_PENDING,
    EXIT_VERIFICATION_PROBLEMS,
    main,
)

from ._helpers import (
    ANCHOR_MS,
    BYBIT_CONTRACT,
    LEGACY_OKX_ID,
    OKX_CONTRACT,
    OKX_ID,
    REPO_ROOT,
    STEP_MS,
    FakeClock,
    FakeExchange,
    legacy_series,
    collector_options,
    mutate_manifest,
    pending_journal,
    sidecar,
    single_pack,
    synthetic_series,
    utc,
    write_legacy_pack,
    write_roster,
)

ISOLATION_CHILD = textwrap.dedent(
    """
    import json, sys

    observed = {"network": [], "writes": []}

    def audit(event, arguments):
        if event.startswith("socket.") or event.startswith("urllib."):
            observed["network"].append(event)
        elif event == "open":
            path, mode = arguments[0], arguments[1]
            if isinstance(mode, str) and any(flag in mode for flag in "wxa+"):
                observed["writes"].append(str(path))

    sys.addaudithook(audit)

    import tools.pattern_lab
    from tools.pattern_lab import collect, data, exchange_data, import_npz, manifest
    from tools.pattern_lab import pack_lock, update_transaction
    from tools.pattern_lab.__main__ import build_parser
    data.require_pyarrow()
    build_parser()
    collect.load_roster()
    exchange_data.adapter_for("OKX")
    exchange_data.adapter_for("BYBIT")
    data.input_fingerprint(
        header=data.fingerprint_header(
            instrument_id="TEST_A", venue="TEST", contract="A", quote_currency="USDT",
            timeframe_minutes=5, start_ms=0, end_ms=300000, warmup_start_ms=0,
        ),
        timestamps=[0], ohlcv=[[1.0, 1.0, 1.0, 1.0, 0.0]],
    )

    forbidden = sorted(
        name for name in sys.modules
        if name.split(".")[0] in {"flask", "werkzeug", "numba", "llvmlite", "core", "strategies", "ui", "optuna"}
    )
    print(json.dumps({"forbidden": forbidden, "observed": observed}))
    """
)

MISSING_PYARROW_CHILD = textwrap.dedent(
    """
    import io, json, sys

    attempts = []
    sys.addaudithook(
        lambda event, arguments: attempts.append(event)
        if event.startswith("subprocess.") or event.startswith("socket.") else None
    )

    class Blocker:
        def find_spec(self, name, path=None, target=None):
            if name == "pyarrow" or name.startswith("pyarrow."):
                raise ImportError("pyarrow is blocked by the test")
            return None

    sys.meta_path.insert(0, Blocker())

    from tools.pattern_lab.__main__ import main

    out, err = io.StringIO(), io.StringIO()
    stdout, stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = out, err
    try:
        code = main(["inspect", "--data-root", sys.argv[1]])
    finally:
        sys.stdout, sys.stderr = stdout, stderr
    print(json.dumps({
        "code": code,
        "stdout": out.getvalue(),
        "stderr": err.getvalue(),
        "installer_loaded": any(name.split(".")[0] in {"pip", "ensurepip"} for name in sys.modules),
        "attempts": attempts,
    }))
    """
)

UNRELATED_IMPORT_CHILD = textwrap.dedent(
    """
    import json, os, sys
    sys.path.insert(0, os.path.join(os.getcwd(), "src"))
    from core import metrics, storage
    print(json.dumps({
        "pattern_lab": [name for name in sys.modules if name.startswith("tools.pattern_lab")],
        "metrics": bool(metrics),
        "storage": bool(storage),
    }))
    """
)


def run_child(script, *arguments):
    return subprocess.run(
        [sys.executable, "-B", "-c", script, *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def run_cli(*arguments):
    return subprocess.run(
        [sys.executable, "-B", "-m", "tools.pattern_lab", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class TestCommandLine:
    def test_help_lists_the_implemented_data_commands(self):
        result = run_cli("--help")
        assert result.returncode == 0
        for command in ("import-npz", "inspect", "slice"):
            assert command in result.stdout

    def test_slice_prints_the_same_metadata_as_the_api(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        start, end = utc(ANCHOR_MS + 12 * STEP_MS), utc(ANCHOR_MS + 72 * STEP_MS)
        result = run_cli(
            "slice",
            "--data-root", str(root),
            "--instrument", "TEST_AAA-USDT-SWAP",
            "--start", start,
            "--end", end,
            "--warmup-start", utc(ANCHOR_MS),
            "--timeframe-minutes", "30",
        )
        assert result.returncode == 0, result.stderr
        assert result.stderr == ""
        payload = json.loads(result.stdout)
        expected = pack_data.slice_metadata(
            pack_data.load_slice(
                root,
                "TEST_AAA-USDT-SWAP",
                start=start,
                end=end,
                warmup_start=utc(ANCHOR_MS),
                timeframe_minutes=30,
            )
        )
        assert payload == expected
        assert payload["input_fingerprint"] == expected["input_fingerprint"]
        assert "open" not in payload  # slice never dumps rows

        # Provenance names what it is: the manifest's declared digest, not a
        # digest of the bytes this read verified.
        provenance = payload["physical_provenance"]
        assert "file_sha256" not in provenance
        assert provenance["declared_file_sha256"] == pack_manifest.read_manifest(root)["instruments"][0]["sha256"]

    def test_inspect_verify_reports_problems_with_a_nonzero_status(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=24)
        assert run_cli("inspect", "--data-root", str(root), "--verify").returncode == 0

        mutate_manifest(root, lambda manifest: manifest["instruments"][0].__setitem__("sha256", "c" * 64))
        result = run_cli("inspect", "--data-root", str(root), "--verify")
        assert result.returncode == 1
        assert "verification problem" in result.stderr
        assert json.loads(result.stdout)["verification_check"]["ok"] is False

    def test_errors_go_to_stderr_with_a_nonzero_status(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=24)
        result = run_cli(
            "slice",
            "--data-root", str(root),
            "--instrument", "TEST_MISSING",
            "--start", utc(ANCHOR_MS),
            "--end", utc(ANCHOR_MS + STEP_MS),
        )
        assert result.returncode == 2
        assert result.stdout == ""
        assert "TEST_MISSING" in result.stderr

    def test_import_emits_a_concise_summary(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()])
        metadata_path = tmp_path / "metadata.json"
        metadata_path.write_text(json.dumps(sidecar()), encoding="utf-8", newline="\n")
        output = tmp_path / "pack"
        result = run_cli(
            "import-npz",
            "--source-root", str(source),
            "--output-root", str(output),
            "--source-metadata", str(metadata_path),
            "--note", "synthetic import",
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["instrument_count"] == 1
        assert payload["instruments"][0]["instrument_id"] == LEGACY_OKX_ID
        assert payload["source_manifest_sha256"] == pack_manifest.file_sha256(source / "MANIFEST.json")
        record = json.loads((output / "updates.jsonl").read_text(encoding="utf-8").strip())
        assert record["note"] == "synthetic import"
        assert record["source"]["kind"] == "legacy_npz_import"

    def test_import_refuses_an_existing_output_root(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()])
        metadata_path = tmp_path / "metadata.json"
        metadata_path.write_text(json.dumps(sidecar()), encoding="utf-8", newline="\n")
        output = tmp_path / "pack"
        output.mkdir()
        assert main(
            [
                "import-npz",
                "--source-root", str(source),
                "--output-root", str(output),
                "--source-metadata", str(metadata_path),
            ]
        ) == 2


class TestImportIsolation:
    def test_clean_import_has_no_application_network_or_write_side_effects(self, tmp_path):
        result = run_child(ISOLATION_CHILD)
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["forbidden"] == []
        assert payload["observed"]["network"] == []
        inside_repository = [
            path for path in payload["observed"]["writes"] if str(REPO_ROOT) in path
        ]
        assert inside_repository == []

    def test_unrelated_merlin_imports_do_not_reach_pattern_lab(self):
        """Merlin code never imports Pattern Lab; the dependency stays one-way.

        Installing PyArrow does not make it a Merlin runtime requirement: pandas
        itself imports PyArrow when it is present, which is pandas behavior and
        independent of this package.
        """
        result = run_child(UNRELATED_IMPORT_CHILD)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["pattern_lab"] == []

    def test_pyarrow_is_referenced_only_by_pattern_lab_sources(self):
        owned = (REPO_ROOT / "tools" / "pattern_lab", REPO_ROOT / "tests" / "pattern_lab")
        offenders = []
        for path in list((REPO_ROOT / "src").rglob("*.py")) + list((REPO_ROOT / "tools").rglob("*.py")) + list(
            (REPO_ROOT / "tests").rglob("*.py")
        ):
            if any(path.is_relative_to(directory) for directory in owned):
                continue
            text = path.read_text(encoding="utf-8")
            if "pyarrow" in text or "pattern_lab" in text:
                offenders.append(str(path.relative_to(REPO_ROOT)))
        assert offenders == []

    def test_missing_pyarrow_is_actionable_nonzero_and_installs_nothing(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=12)
        result = run_child(MISSING_PYARROW_CHILD, str(root))
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["code"] == 2
        assert payload["stdout"] == ""
        assert "pyarrow==22.0.0" in payload["stderr"]
        assert "never installs dependencies automatically" in payload["stderr"]
        assert payload["installer_loaded"] is False
        assert payload["attempts"] == []

    def test_root_requirements_pin_the_new_dependency(self):
        requirements = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
        assert "pyarrow==22.0.0" in requirements
        assert len([line for line in requirements if line.startswith("pyarrow")]) == 1


# --------------------------------------------------------------------------
# collector command line
# --------------------------------------------------------------------------


@pytest.fixture
def wired(monkeypatch):
    """Route the CLI's default transport and clock at the injection boundary."""
    stamps, values = synthetic_series(40)
    now_ms = int(stamps[-1]) + STEP_MS + 90_000
    exchange = FakeExchange(now_ms=now_ms)
    exchange.add_okx(OKX_CONTRACT, timestamps=stamps, ohlcv=values)
    exchange.add_bybit(BYBIT_CONTRACT, timestamps=stamps, ohlcv=values)
    clock = FakeClock(now_ms)
    monkeypatch.setattr(exchange_data, "urllib_transport", exchange)
    monkeypatch.setattr(exchange_data, "SystemClock", lambda: clock)
    return exchange, clock, stamps


def call(capsys, *arguments):
    """Run the CLI in process and return ``(code, stdout_json, stderr)``."""
    code = main(list(arguments))
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out.strip() else None
    return code, payload, captured.err


class TestCollectorCommandLine:
    def test_help_lists_the_collector_commands(self):
        result = run_cli("--help")
        assert result.returncode == 0
        for command in ("collect", "update", "recover", "abort-update"):
            assert command in result.stdout

    def test_collect_prints_a_structured_result_and_exits_zero(self, tmp_path, capsys, wired):
        roster = write_roster(tmp_path / "universe.json")
        code, payload, err = call(
            capsys,
            "collect",
            "--universe",
            str(roster),
            "--data-root",
            str(tmp_path / "pack"),
            "--start",
            utc(ANCHOR_MS),
            "--end",
            "latest-closed",
        )
        assert code == EXIT_OK
        assert payload["status"] == "completed"
        assert payload["kind"] == "collect"
        assert payload["revision_before"] is None and payload["revision_after"] == 1
        assert payload["resolved_request"]["requested_end_token"] == "latest-closed"
        assert len(payload["instruments"]) == 2
        assert payload["http_request_count"] > 0
        assert "pattern-lab:" in err  # progress diagnostics stay on stderr

    def test_a_tail_shortfall_publishes_but_exits_one(self, tmp_path, capsys, wired):
        exchange, clock, stamps = wired
        exchange.now_ms = clock.now_ms = int(stamps[-1]) + 30 * STEP_MS
        roster = write_roster(tmp_path / "universe.json")
        code, payload, err = call(
            capsys,
            "collect",
            "--universe",
            str(roster),
            "--data-root",
            str(tmp_path / "pack"),
            "--start",
            utc(ANCHOR_MS),
            "--end",
            "latest-closed",
        )
        assert code == EXIT_VERIFICATION_PROBLEMS
        assert payload["status"] == "completed"
        assert payload["tail_shortfall_bars"] > 0
        assert "the requested range is not complete" in err

    def test_a_pending_operation_makes_an_update_exit_four(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        roster = write_roster(tmp_path / "universe.json")
        assert call(
            capsys, "collect", "--universe", str(roster), "--data-root", str(root),
            "--start", utc(ANCHOR_MS), "--end", "latest-closed",
        )[0] == EXIT_OK
        update_transaction.write_journal(root, pending_journal(root))
        code, payload, err = call(capsys, "update", "--data-root", str(root), "--end", "latest-closed")
        assert code == EXIT_PENDING
        assert payload["error_code"] == "pending_operation"
        assert payload["status"] == "failed"
        assert "recover" in err

    def test_a_busy_pack_exits_three_without_mutating_anything(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        roster = write_roster(tmp_path / "universe.json")
        call(
            capsys, "collect", "--universe", str(roster), "--data-root", str(root),
            "--start", utc(ANCHOR_MS), "--end", "latest-closed",
        )
        before = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        with pack_lock.pack_guard(root):
            code, payload, err = call(
                capsys, "update", "--data-root", str(root), "--end", "latest-closed"
            )
        assert code == EXIT_BUSY
        assert payload["error_code"] == "pack_busy"
        after = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        assert after == before

    def test_inspect_reports_a_pending_initial_collect_without_a_manifest(
        self, tmp_path, capsys, wired, monkeypatch
    ):
        root = tmp_path / "pack"
        roster = write_roster(tmp_path / "universe.json")
        original = pack_collect._collect_instrument
        calls = {"n": 0}

        def wrapper(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("interrupted")
            return original(*args, **kwargs)

        monkeypatch.setattr(pack_collect, "_collect_instrument", wrapper)
        with pytest.raises(RuntimeError):
            call(
                capsys, "collect", "--universe", str(roster), "--data-root", str(root),
                "--start", utc(ANCHOR_MS), "--end", "latest-closed",
            )
        # Restore only the interrupted helper: the wired transport must stay patched.
        monkeypatch.setattr(pack_collect, "_collect_instrument", original)

        code, payload, _ = call(capsys, "inspect", "--data-root", str(root))
        assert code == EXIT_OK
        assert payload["manifest_present"] is False
        assert payload["pending_operation"]["phase"] == "staging"

        code, payload, err = call(capsys, "inspect", "--data-root", str(root), "--verify")
        assert code == EXIT_VERIFICATION_PROBLEMS
        assert payload["verification_check"] == {
            "checked": False,
            "ok": False,
            "problems": payload["verification_check"]["problems"],
        }
        assert "verification problem" in err

        code, payload, _ = call(capsys, "recover", "--data-root", str(root))
        assert code == EXIT_OK
        assert payload["status"] == "recovered"

    def test_abort_update_leaves_a_reusable_lock_only_root(self, tmp_path, capsys, wired, monkeypatch):
        root = tmp_path / "pack"
        roster = write_roster(tmp_path / "universe.json")
        original = pack_collect._collect_instrument

        def wrapper(*args, **kwargs):
            raise RuntimeError("interrupted")

        monkeypatch.setattr(pack_collect, "_collect_instrument", wrapper)
        with pytest.raises(RuntimeError):
            call(
                capsys, "collect", "--universe", str(roster), "--data-root", str(root),
                "--start", utc(ANCHOR_MS), "--end", "latest-closed",
            )
        monkeypatch.setattr(pack_collect, "_collect_instrument", original)

        code, payload, _ = call(capsys, "abort-update", "--data-root", str(root))
        assert code == EXIT_OK
        assert payload["status"] == "aborted"
        assert sorted(item.name for item in root.iterdir()) == [pack_lock.LOCK_NAME]

    def test_recover_without_a_pending_operation_is_reported_plainly(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        roster = write_roster(tmp_path / "universe.json")
        call(
            capsys, "collect", "--universe", str(roster), "--data-root", str(root),
            "--start", utc(ANCHOR_MS), "--end", "latest-closed",
        )
        code, payload, _ = call(capsys, "recover", "--data-root", str(root))
        assert code == EXIT_OK
        assert payload["status"] == "nothing_to_recover"

    @pytest.mark.parametrize("flag, value", [("--okx-rps", "0"), ("--bybit-rps", "500")])
    def test_invalid_pacing_options_are_refused(self, tmp_path, capsys, wired, flag, value):
        roster = write_roster(tmp_path / "universe.json")
        code, payload, err = call(
            capsys, "collect", "--universe", str(roster), "--data-root", str(tmp_path / "pack"),
            "--start", utc(ANCHOR_MS), "--end", "latest-closed", flag, value,
        )
        assert code == EXIT_ERROR
        assert payload["status"] == "failed"
        assert "pattern-lab:" in err

    def test_an_existing_pack_blocks_a_new_collect(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        roster = write_roster(tmp_path / "universe.json")
        call(
            capsys, "collect", "--universe", str(roster), "--data-root", str(root),
            "--start", utc(ANCHOR_MS), "--end", "latest-closed",
        )
        code, payload, _ = call(
            capsys, "collect", "--universe", str(roster), "--data-root", str(root),
            "--start", utc(ANCHOR_MS), "--end", "latest-closed",
        )
        assert code == EXIT_ERROR
        assert payload["error_code"] == "destination_not_empty"

    def test_collect_help_documents_the_resolved_end_token(self):
        result = run_cli("collect", "--help")
        assert result.returncode == 0
        assert "latest-closed" in result.stdout


class TestPendingReadExitCodes:
    """A valid pending operation is exit 4 everywhere a research read is refused."""

    def _collect(self, capsys, tmp_path, root):
        roster = write_roster(tmp_path / "universe.json")
        assert call(
            capsys, "collect", "--universe", str(roster), "--data-root", str(root),
            "--start", utc(ANCHOR_MS), "--end", "latest-closed",
        )[0] == EXIT_OK
        return roster

    def _slice(self, capsys, root):
        return call(
            capsys, "slice", "--data-root", str(root), "--instrument", OKX_ID,
            "--start", utc(ANCHOR_MS), "--end", utc(ANCHOR_MS + 10 * STEP_MS),
        )

    def test_a_normal_root_reads_and_inspects_cleanly(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        self._collect(capsys, tmp_path, root)
        assert self._slice(capsys, root)[0] == EXIT_OK
        assert call(capsys, "inspect", "--data-root", str(root))[0] == EXIT_OK
        assert call(capsys, "inspect", "--data-root", str(root), "--verify")[0] == EXIT_OK

    def test_a_pending_update_makes_a_slice_exit_four(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        self._collect(capsys, tmp_path, root)
        update_transaction.write_journal(root, pending_journal(root))

        code, _, err = self._slice(capsys, root)
        assert code == EXIT_PENDING
        assert "recover" in err and "abort-update" in err
        # A plain inspect still reports the pending status with 0 ...
        status, report, _ = call(capsys, "inspect", "--data-root", str(root))
        assert status == EXIT_OK
        assert report["pending_operation"]["source"] == "marker"
        # ... while --verify refuses to certify it.
        code, report, _ = call(capsys, "inspect", "--data-root", str(root), "--verify")
        assert code == EXIT_VERIFICATION_PROBLEMS
        assert report["verification_check"]["checked"] is False
        assert report["verification_check"]["ok"] is False

    def test_a_pending_initial_collect_exits_four_before_a_manifest_is_required(
        self, tmp_path, capsys, wired
    ):
        root = tmp_path / "fresh"
        exchange, clock, _ = wired
        roster = write_roster(tmp_path / "universe.json")
        with pytest.raises(RuntimeError):
            pack_collect.collect_pack(
                root, start=utc(ANCHOR_MS), end="latest-closed", roster_path=roster,
                options=collector_options(), transport=exchange, clock=clock,
                progress=_fail_once_staging(),
            )
        assert not pack_manifest.manifest_path(root).is_file()

        code, _, err = self._slice(capsys, root)
        assert code == EXIT_PENDING  # a pending first collect, not a missing pack
        assert "recover" in err
        status, report, _ = call(capsys, "inspect", "--data-root", str(root))
        assert status == EXIT_OK
        assert report["manifest_present"] is False
        assert report["pending_operation"]["kind"] == "collect"

    def test_the_initial_journal_temporary_also_exits_four(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        self._collect(capsys, tmp_path, root)
        journal = update_transaction.write_journal(root, pending_journal(root))
        update_transaction.marker_path(root).rename(update_transaction.marker_temp_path(root))
        assert journal["phase"] == "staging"

        code, _, err = self._slice(capsys, root)
        assert code == EXIT_PENDING
        assert "recover" in err

    def test_a_malformed_marker_stays_invalid_input(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        self._collect(capsys, tmp_path, root)
        update_transaction.marker_path(root).write_text("{ not json", encoding="utf-8")
        code, _, err = self._slice(capsys, root)
        assert code == EXIT_ERROR
        assert "malformed pending-operation journal" in err

    def test_a_busy_root_still_exits_three(self, tmp_path, capsys, wired):
        root = tmp_path / "pack"
        self._collect(capsys, tmp_path, root)
        update_transaction.write_journal(root, pending_journal(root))
        with pack_lock.pack_guard(root):
            code, _, err = self._slice(capsys, root)
        assert code == EXIT_BUSY  # busy is decided before the pending state
        assert "pack lock" in err


def _fail_once_staging():
    """Return a progress sink that interrupts an operation after its journal exists."""

    def progress(message):
        if message.startswith("preflight"):
            raise RuntimeError("simulated interruption after the journal was written")

    return progress
