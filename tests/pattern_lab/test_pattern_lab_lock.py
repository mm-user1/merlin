"""Cooperative process exclusion, read sessions and pending-generation refusal.

The busy cases use real separate processes with bounded timeouts and task-owned
storage under the launcher's external temporary root.  No production process is
ever signalled.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import time

import pytest

from tools.pattern_lab import PatternLabBusyError, PatternLabDataError
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import import_npz as pack_import
from tools.pattern_lab import manifest as pack_manifest
from tools.pattern_lab import pack_lock
from tools.pattern_lab import update_transaction

from ._helpers import (
    ANCHOR_MS,
    STEP_MS,
    REPO_ROOT,
    legacy_series,
    sidecar,
    single_pack,
    synthetic_series,
    instrument_source,
    publish,
    utc,
    write_legacy_pack,
)

CHILD_TIMEOUT_SECONDS = 30
READY_TIMEOUT_SECONDS = 20

HOLDER = textwrap.dedent(
    """
    import os, sys, time
    from tools.pattern_lab import pack_lock

    root, ready, release = sys.argv[1], sys.argv[2], sys.argv[3]
    with pack_lock.pack_guard(root):
        open(ready, "w").close()
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline and not os.path.exists(release):
            time.sleep(0.02)
    """
)

DYING_HOLDER = textwrap.dedent(
    """
    import json, os, sys
    from tools.pattern_lab import pack_lock

    root, journal, ready = sys.argv[1], sys.argv[2], sys.argv[3]
    guard = pack_lock.pack_guard(root)
    guard.__enter__()
    with open(os.path.join(root, ".update-in-progress.json"), "w", encoding="utf-8") as handle:
        handle.write(open(journal, encoding="utf-8").read())
    open(ready, "w").close()
    os._exit(9)  # die while still holding the OS guard
    """
)

BUSY_CALLER = textwrap.dedent(
    """
    import json, sys
    from tools.pattern_lab import PatternLabBusyError
    from tools.pattern_lab import data as pack_data

    root, action = sys.argv[1], sys.argv[2]
    try:
        if action == "read":
            pack_data.load_slice(root, sys.argv[3], start=sys.argv[4], end=sys.argv[5])
        elif action == "inspect":
            pack_data.inspect_pack(root)
        else:
            raise AssertionError(action)
    except PatternLabBusyError as exc:
        print(json.dumps({"busy": True, "message": str(exc)}))
    else:
        print(json.dumps({"busy": False, "message": ""}))
    """
)


def start_child(script, *arguments):
    return subprocess.Popen(
        [sys.executable, "-B", "-c", script, *arguments],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def run_child(script, *arguments):
    return subprocess.run(
        [sys.executable, "-B", "-c", script, *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=CHILD_TIMEOUT_SECONDS,
        check=False,
    )


def wait_for(path: Path) -> None:
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.02)
    raise AssertionError(f"{path}: the child never signalled readiness")


def stop_child(process, release: Path) -> None:
    release.write_text("go", encoding="utf-8")
    try:
        process.wait(timeout=CHILD_TIMEOUT_SECONDS)
    finally:
        if process.poll() is None:  # pragma: no cover - only on a hung child
            process.kill()
            process.wait(timeout=CHILD_TIMEOUT_SECONDS)


class TestLockFile:
    def test_the_lock_file_is_created_once_and_never_truncated(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=20)
        path = pack_lock.lock_path(root)
        with pack_lock.pack_guard(root):
            pass
        assert path.is_file()
        path.write_bytes(b"operator marker")
        with pack_lock.pack_guard(root):
            pass
        assert path.read_bytes() == b"operator marker"

    def test_a_read_does_not_create_a_missing_root(self, tmp_path):
        with pytest.raises(PatternLabDataError, match="not an existing directory"):
            pack_data.inspect_pack(tmp_path / "absent")
        assert not (tmp_path / "absent").exists()

    def test_a_symlinked_lock_file_is_refused(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=20)
        pack_lock.lock_path(root).unlink(missing_ok=True)
        target = tmp_path / "elsewhere"
        target.write_text("", encoding="utf-8")
        pack_lock.lock_path(root).symlink_to(target)
        with pytest.raises(PatternLabDataError, match="must not be a symlink"):
            pack_data.inspect_pack(root)

    def test_an_unwritable_root_names_the_permission_requirement(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=20)
        pack_lock.lock_path(root).unlink(missing_ok=True)
        mode = root.stat().st_mode
        os.chmod(root, 0o500)
        try:
            if os.access(root, os.W_OK):  # pragma: no cover - running as root
                pytest.skip("the test user can write to a read-only directory")
            with pytest.raises(PatternLabDataError, match="write permission inside the data root"):
                pack_data.inspect_pack(root)
        finally:
            os.chmod(root, mode)

    def test_the_root_identity_resolves_through_an_existing_parent(self, tmp_path):
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path / "real")
        (tmp_path / "real").mkdir()
        assert pack_lock.resolve_root_identity(alias / "pack") == os.path.normcase(
            str((tmp_path / "real" / "pack").resolve())
        )

    def test_a_missing_parent_is_named(self, tmp_path):
        with pytest.raises(PatternLabDataError, match="parent directory"):
            pack_lock.resolve_root_identity(tmp_path / "absent" / "pack")


class TestProcessExclusion:
    def test_a_held_root_refuses_a_second_reader(self, tmp_path):
        root = tmp_path / "pack"
        stamps, _ = single_pack(root, slot_count=40)
        ready, release = tmp_path / "ready", tmp_path / "release"
        child = start_child(HOLDER, str(root), str(ready), str(release))
        try:
            wait_for(ready)
            result = run_child(
                BUSY_CALLER,
                str(root),
                "read",
                "TEST_AAA-USDT-SWAP",
                utc(int(stamps[0])),
                utc(int(stamps[10])),
            )
            assert result.returncode == 0, result.stderr
            payload = json.loads(result.stdout)
            assert payload["busy"] is True
            assert "pack lock" in payload["message"]
            assert run_child(BUSY_CALLER, str(root), "inspect").stdout.strip().startswith('{"busy": true')
        finally:
            stop_child(child, release)

    def test_two_independent_readers_conflict_deliberately(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=20)
        with pack_lock.pack_guard(root):
            with pytest.raises(PatternLabBusyError):
                with pack_lock.pack_guard(root):
                    pass

    def test_a_read_session_prevents_a_between_read_update(self, tmp_path):
        root = tmp_path / "pack"
        stamps, _ = single_pack(root, slot_count=40)
        with pack_data.read_session(root) as session:
            first = session.load_slice(
                "TEST_AAA-USDT-SWAP", start=utc(int(stamps[0])), end=utc(int(stamps[10]))
            )
            blocked = run_child(BUSY_CALLER, str(root), "inspect")
            assert json.loads(blocked.stdout)["busy"] is True
            second = session.load_slice(
                "TEST_AAA-USDT-SWAP", start=utc(int(stamps[0])), end=utc(int(stamps[10]))
            )
        assert first.input_fingerprint == second.input_fingerprint
        assert session.inspect()["revision"] == 1

    def test_process_death_releases_the_guard_but_a_journal_still_blocks_reads(self, tmp_path):
        root = tmp_path / "pack"
        stamps, _ = single_pack(root, slot_count=40)
        journal = tmp_path / "journal.json"
        journal.write_text(
            pack_manifest.dumps_json(_minimal_journal(root)) + "\n", encoding="utf-8"
        )
        ready = tmp_path / "ready"
        child = start_child(DYING_HOLDER, str(root), str(journal), str(ready))
        wait_for(ready)
        child.wait(timeout=CHILD_TIMEOUT_SECONDS)
        assert child.returncode == 9

        # The OS released the guard at process exit, so the lock is free again.
        with pack_lock.pack_guard(root):
            pass
        with pytest.raises(PatternLabDataError, match="pending collector operation"):
            pack_data.load_slice(
                root, "TEST_AAA-USDT-SWAP", start=utc(int(stamps[0])), end=utc(int(stamps[10]))
            )
        report = pack_data.inspect_pack(root, verify=True)
        assert report["update_in_progress"] is True
        assert report["verification_check"] == {
            "checked": False,
            "ok": False,
            "problems": report["verification_check"]["problems"],
        }
        assert "recover" in report["verification_check"]["problems"][0]


class TestNoNestedSelfLock:
    def test_publish_and_import_take_the_lock_exactly_once(self, tmp_path):
        source = tmp_path / "legacy"
        write_legacy_pack(source, [legacy_series()])
        output = tmp_path / "converted"
        summary = pack_import.import_npz_pack(
            source, output, source_metadata=sidecar(), note="import"
        )
        assert summary["instrument_count"] == 1
        assert pack_lock.lock_path(output).is_file()

        stamps, values = synthetic_series(20)
        published = tmp_path / "published"
        publish(published, [instrument_source(stamps, values)])
        assert pack_manifest.manifest_path(published).is_file()

    def test_an_update_style_read_inside_a_guard_uses_the_private_core(self, tmp_path):
        root = tmp_path / "pack"
        stamps, _ = single_pack(root, slot_count=40)
        with pack_lock.pack_guard(root):
            report = pack_data._inspect_pack_unlocked(root, verify=True)
            loaded = pack_data._load_slice_unlocked(
                root, "TEST_AAA-USDT-SWAP", start=utc(int(stamps[0])), end=utc(int(stamps[10]))
            )
        assert report["verification_check"]["ok"] is True
        assert len(loaded.bars) == 10


class TestDestinationCreation:
    def test_publish_refuses_a_pre_existing_destination(self, tmp_path):
        root = tmp_path / "pack"
        root.mkdir()
        stamps, values = synthetic_series(20)
        with pytest.raises(PatternLabDataError, match="already exists"):
            publish(root, [instrument_source(stamps, values)])

    def test_a_competing_creator_cannot_overwrite_a_locked_destination(self, tmp_path):
        root = tmp_path / "pack"
        root.mkdir()
        pack_lock.lock_path(root).touch()
        (root / pack_manifest.UPDATE_MARKER_NAME).write_text("{}", encoding="utf-8")
        stamps, values = synthetic_series(20)
        with pytest.raises(PatternLabDataError, match="already exists"):
            publish(root, [instrument_source(stamps, values)])


def _minimal_journal(root: Path) -> dict:
    """Build a valid staging journal for an update of a one-instrument pack."""
    roster = [
        {
            "contract": "AAA-USDT-SWAP",
            "instrument_id": "TEST_AAA-USDT-SWAP",
            "quote_currency": "USDT",
            "roles": ["trading"],
            "symbol": "AAA",
            "venue": "TEST",
        }
    ]
    operation_id = update_transaction.new_operation_id(
        pack_manifest.parse_utc("2026-09-14T00:00:00Z", "moment")
    )
    manifest = pack_manifest.read_manifest(root)
    return {
        "journal_version": 1,
        "operation_id": operation_id,
        "kind": "update",
        "root": pack_lock.resolve_root_identity(root),
        "staging_dir": update_transaction.staging_dir_name(operation_id),
        "operation_started_utc": "2026-09-14T00:00:00Z",
        "request": {
            "start_utc": utc(ANCHOR_MS),
            "end_utc": utc(ANCHOR_MS + 40 * STEP_MS),
            "requested_end_token": None,
            "requested_start_utc": None,
            "note": None,
        },
        "options": {"okx_rps": 2.0, "bybit_rps": 2.0, "timeout_seconds": 20.0, "max_attempts": 5},
        "universe": dict(manifest["universe"]),
        "roster": roster,
        "roster_sha256": pack_manifest.roster_sha256(roster),
        "base": {
            "revision": manifest["revision"],
            "manifest_sha256": pack_manifest.file_sha256(pack_manifest.manifest_path(root)),
            "readme_sha256": pack_manifest.file_sha256(root / pack_manifest.README_NAME),
            "updates_sha256": pack_manifest.file_sha256(root / pack_manifest.UPDATES_NAME),
            "files": {entry["instrument_id"]: entry["sha256"] for entry in manifest["instruments"]},
        },
        "target_revision": manifest["revision"] + 1,
        "phase": "staging",
        "closure": {},
        "preflight": {},
        "staged": {},
        "targets": None,
    }
