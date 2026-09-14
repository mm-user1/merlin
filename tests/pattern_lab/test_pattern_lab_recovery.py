"""Journal validation, interruption recovery, abort and error safety.

Every scenario interrupts a real operation at a named boundary, then finishes it
with ``recover`` or discards it with ``abort-update`` and checks that exactly one
revision and one history event were published.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabPendingError
from tools.pattern_lab import collect as pack_collect
from tools.pattern_lab import data as pack_data
from tools.pattern_lab import manifest as pack_manifest
from tools.pattern_lab import pack_lock
from tools.pattern_lab import update_transaction

from ._helpers import (
    ANCHOR_MS,
    BYBIT_CONTRACT,
    BYBIT_ID,
    OKX_CONTRACT,
    OKX_ID,
    STEP_MS,
    build_exchange,
    recover,
    run_collect,
    run_update,
    synthetic_series,
    utc,
    write_roster,
)


class Interrupted(RuntimeError):
    """A simulated process interruption at a named transaction boundary."""


def fail_before(monkeypatch, module, name, predicate):
    """Raise instead of running ``name`` once ``predicate`` matches."""
    original = getattr(module, name)
    state = {"calls": 0}

    def wrapper(*args, **kwargs):
        state["calls"] += 1
        if predicate(state["calls"], args, kwargs):
            raise Interrupted(f"interrupted before {name} call {state['calls']}")
        return original(*args, **kwargs)

    monkeypatch.setattr(module, name, wrapper)
    return state


def fail_after(monkeypatch, module, name, predicate):
    """Run ``name`` and then raise, once ``predicate`` matches."""
    original = getattr(module, name)
    state = {"calls": 0}

    def wrapper(*args, **kwargs):
        state["calls"] += 1
        result = original(*args, **kwargs)
        if predicate(state["calls"], args, kwargs):
            raise Interrupted(f"interrupted after {name} call {state['calls']}")
        return result

    monkeypatch.setattr(module, name, wrapper)
    return state


@pytest.fixture
def roster(tmp_path):
    return write_roster(tmp_path / "universe.json")


@pytest.fixture
def collected(tmp_path, roster):
    """A published two-instrument pack plus a grown source ready for an update."""
    root = tmp_path / "pack"
    exchange, clock, _, _ = build_exchange(slots=40)
    run_collect(root, roster, exchange, clock)
    longer_stamps, longer_values = synthetic_series(60)
    exchange.now_ms = clock.now_ms = int(longer_stamps[-1]) + STEP_MS + 90_000
    exchange.set_rows("OKX", OKX_CONTRACT, longer_stamps, longer_values)
    exchange.set_rows("BYBIT", BYBIT_CONTRACT, longer_stamps, longer_values)
    exchange.requests.clear()
    return root, exchange, clock


def history_lines(root: Path) -> list[dict]:
    path = Path(root) / pack_manifest.UPDATES_NAME
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def assert_published_once(root: Path, *, revision: int, events: int) -> None:
    manifest = pack_manifest.read_manifest(root)
    assert manifest["revision"] == revision
    assert len(history_lines(root)) == events
    assert update_transaction.pending_state(root) is None
    assert pack_data.verify_instrument_files(root, manifest) == []
    assert (root / pack_manifest.README_NAME).read_text(encoding="utf-8") == (
        pack_manifest.render_readme(manifest)
    )
    assert not list(root.glob(f"{update_transaction.STAGING_PREFIX}*"))


class TestJournalValidation:
    def test_a_valid_journal_round_trips(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        state = update_transaction.pending_state(root)
        journal = state["journal"]
        assert journal["phase"] == "applying"
        assert journal["kind"] == "update"
        assert journal["root"] == pack_lock.resolve_root_identity(root)
        assert sorted(journal["staged"]) == [BYBIT_ID, OKX_ID]
        assert update_transaction.validate_journal(journal) == journal

    @pytest.mark.parametrize(
        "mutate, message",
        [
            (lambda j: j.update(journal_version=2), "unsupported version"),
            (lambda j: j.update(kind="delete"), "expected one of"),
            (lambda j: j.update(phase="done"), "expected one of"),
            (lambda j: j.update(operation_id="../escape"), "unsafe operation identifier"),
            (lambda j: j.update(staging_dir="elsewhere"), "staging_dir"),
            (lambda j: j.update(extra=1), "unexpected journal keys"),
            (lambda j: j.update(roster_sha256="0" * 64), "does not match the recorded roster"),
            (lambda j: j["staged"][OKX_ID].update(staged_path="../outside/x.parquet"), "must be"),
            (lambda j: j["staged"][OKX_ID].update(final_path="/etc/passwd"), "relative path"),
            (lambda j: j["targets"]["manifest"].update(staged_path="ohlcv/x"), "must be"),
            (lambda j: j["targets"].pop("readme"), "expected exactly"),
            (lambda j: j.update(target_revision=99), "publishes 2"),
            (lambda j: j["base"].update(revision=None), "must record the base revision"),
            (lambda j: j["request"].update(end_utc=utc(ANCHOR_MS + 5 * STEP_MS)), "does not agree"),
            (lambda j: j["request"].update(requested_end_token="now"), "requested_end_token"),
            (lambda j: j["closure"].update(safe_cutoff_ms=j["closure"]["safe_cutoff_ms"] + STEP_MS),
             "is not the cutoff"),
            (lambda j: j["closure"].pop("observed_utc"), "closure object is closed"),
            (lambda j: j["options"].update(max_attempts=9), "max_attempts"),
            (lambda j: j["options"].pop("okx_rps"), "options object is closed"),
            (lambda j: j["staged"].update(FOREIGN_ID=j["staged"][OKX_ID]), "not a member"),
            (lambda j: j["preflight"].update(FOREIGN_ID={}), "not a member"),
            (lambda j: j["staged"].pop(BYBIT_ID), "were never completed"),
            (lambda j: j["staged"][OKX_ID]["facts"].update(row_count=1), "does not agree"),
            (lambda j: j["staged"][OKX_ID].update(old_sha256="c" * 64), "recorded base digest"),
            (lambda j: j["staged"][OKX_ID].update(new_sha256="c" * 64), "does not match the recorded"),
            (
                lambda j: j["staged"][OKX_ID].update(
                    staged_path=f"{j['staging_dir']}/BYBIT_BBBUSDT_5m.parquet"
                ),
                "staged_path: must be",
            ),
        ],
    )
    def test_malformed_journals_are_rejected(self, collected, monkeypatch, mutate, message):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        journal = update_transaction.pending_state(root)["journal"]
        mutate(journal)
        with pytest.raises(PatternLabDataError, match=message):
            update_transaction.validate_journal(journal)

    def test_a_malformed_marker_is_reported_and_never_adopted(self, collected):
        root, _, _ = collected
        marker = update_transaction.marker_path(root)
        marker.write_text("{ not json", encoding="utf-8")
        with pytest.raises(PatternLabDataError, match="malformed pending-operation journal"):
            pack_data.inspect_pack(root)
        with pytest.raises(PatternLabDataError, match="malformed pending-operation journal"):
            pack_collect.recover_pack(root)
        assert marker.is_file()

    def test_a_symlinked_staging_directory_is_refused(self, collected, tmp_path, monkeypatch):
        root, exchange, clock = collected
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        original = update_transaction.ensure_staging_dir

        def link_staging(data_root, operation_id):
            path = update_transaction.staging_dir(data_root, operation_id)
            if not path.exists():
                path.symlink_to(elsewhere)
            return original(data_root, operation_id)

        monkeypatch.setattr(update_transaction, "ensure_staging_dir", link_staging)
        with pytest.raises(PatternLabDataError, match="must not be a symlink"):
            run_update(root, exchange, clock)


class TestStagingRecovery:
    def test_recovery_after_a_completed_stage_reuses_it(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(
            monkeypatch, pack_collect, "_collect_instrument", lambda calls, *_: calls == 2
        )
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        journal = update_transaction.pending_state(root)["journal"]
        assert sorted(journal["staged"]) == [BYBIT_ID]
        assert journal["phase"] == "staging"
        assert journal["preflight"]  # completed preflight evidence is journalled

        exchange.requests.clear()
        result = recover(root, exchange, clock)
        assert result["status"] == "recovered"
        paths = [path for path, _ in exchange.requests]
        assert "/v5/market/kline" not in paths  # the completed Bybit stage is reused
        assert "/api/v5/market/history-candles" in paths
        assert "/v5/market/instruments-info" not in paths  # preflight is not repeated
        assert_published_once(root, revision=2, events=2)

    def test_an_interrupted_initial_collect_is_inspectable_without_a_manifest(
        self, tmp_path, roster, monkeypatch
    ):
        root = tmp_path / "fresh"
        exchange, clock, _, _ = build_exchange(slots=40)
        fail_before(
            monkeypatch, pack_collect, "_collect_instrument", lambda calls, *_: calls == 2
        )
        with pytest.raises(Interrupted):
            run_collect(root, roster, exchange, clock)

        assert not pack_manifest.manifest_path(root).is_file()
        report = pack_data.inspect_pack(root)
        assert report["manifest_present"] is False
        assert report["update_in_progress"] is True
        pending = report["pending_operation"]
        assert pending["kind"] == "collect"
        assert pending["phase"] == "staging"
        assert pending["completed_instruments"] == [BYBIT_ID]
        assert report["verification_check"] == {"checked": False, "ok": None, "problems": []}

        verified = pack_data.inspect_pack(root, verify=True)
        assert verified["verification_check"]["checked"] is False
        assert verified["verification_check"]["ok"] is False

        result = recover(root, exchange, clock)
        assert result["status"] == "recovered"
        assert_published_once(root, revision=1, events=1)

    def test_an_initial_collect_reuses_its_completed_stage(self, tmp_path, roster, monkeypatch):
        root = tmp_path / "fresh"
        exchange, clock, _, _ = build_exchange(slots=40)
        fail_before(
            monkeypatch, pack_collect, "_collect_instrument", lambda calls, *_: calls == 2
        )
        with pytest.raises(Interrupted):
            run_collect(root, roster, exchange, clock)
        monkeypatch.undo()
        journal = update_transaction.pending_state(root)["journal"]
        assert journal["kind"] == "collect" and sorted(journal["staged"]) == [BYBIT_ID]
        staged = root / journal["staged"][BYBIT_ID]["staged_path"]
        digest = pack_manifest.file_sha256(staged)

        exchange.requests.clear()
        recover(root, exchange, clock)
        assert_published_once(root, revision=1, events=1)
        # The completed instrument was reused by digest, not downloaded again.
        assert pack_manifest.file_sha256(
            root / pack_manifest.instrument_relative_file(BYBIT_ID)
        ) == digest
        downloaded = {params.get("symbol") or params.get("instId") for _, params in exchange.requests}
        assert BYBIT_CONTRACT not in downloaded
        assert OKX_CONTRACT in downloaded

    def test_a_staged_artifact_that_vanished_is_refetched(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(
            monkeypatch, pack_collect, "_collect_instrument", lambda calls, *_: calls == 2
        )
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        journal = update_transaction.pending_state(root)["journal"]
        staged = root / journal["staged"][BYBIT_ID]["staged_path"]
        staged.unlink()

        exchange.requests.clear()
        recover(root, exchange, clock)
        assert "/v5/market/kline" in [path for path, _ in exchange.requests]
        assert_published_once(root, revision=2, events=2)


class TestApplyingRecovery:
    @pytest.mark.parametrize(
        "module, name, predicate, label",
        [
            (update_transaction, "apply_operation", lambda calls, *_: True, "entering applying"),
            (
                update_transaction,
                "_publish_replacement",
                lambda calls, args, kwargs: calls == 2,
                "between file replacements",
            ),
            (
                update_transaction,
                "_publish_replacement",
                lambda calls, args, kwargs: args[0].destination.name == pack_manifest.MANIFEST_NAME,
                "after history and README",
            ),
            (update_transaction, "cleanup_operation", lambda calls, *_: True, "after the manifest"),
            (update_transaction, "remove_marker", lambda calls, *_: True, "during cleanup"),
        ],
    )
    def test_recovery_finishes_forward_exactly_once(
        self, collected, monkeypatch, module, name, predicate, label
    ):
        root, exchange, clock = collected
        fail_before(monkeypatch, module, name, predicate)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        assert update_transaction.pending_state(root) is not None
        with pytest.raises(PatternLabPendingError, match="pending collector operation"):
            pack_data.load_slice(
                root, OKX_ID, start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 10 * STEP_MS)
            )
        monkeypatch.undo()

        exchange.requests.clear()
        result = recover(root, exchange, clock)
        assert result["status"] == "recovered", label
        assert exchange.requests == [], f"{label}: applying recovery must not re-download"
        assert_published_once(root, revision=2, events=2)

    def test_cleanup_is_idempotent_for_consumed_artifacts(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "remove_marker", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        # Cleanup already consumed every staged artifact; only the marker remains.
        journal = update_transaction.pending_state(root)["journal"]
        assert journal["target_revision"] == 2
        assert not list(root.glob(f"{update_transaction.STAGING_PREFIX}*"))
        recover(root, exchange, clock)
        assert_published_once(root, revision=2, events=2)
        # Running recovery again is a clean no-op, not a second revision.
        assert recover(root, exchange, clock)["status"] == "nothing_to_recover"
        assert_published_once(root, revision=2, events=2)

    def test_an_unexpected_destination_hash_blocks_recovery(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        target = root / pack_manifest.instrument_relative_file(OKX_ID)
        target.write_bytes(target.read_bytes() + b"tampered")
        with pytest.raises(PatternLabDataError) as failure:
            recover(root, exchange, clock)
        assert failure.value.error_code == "unexpected_target_state"
        assert update_transaction.pending_state(root) is not None

    def test_a_missing_staged_artifact_stops_recovery_without_destroying_evidence(
        self, collected, monkeypatch
    ):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        journal = update_transaction.pending_state(root)["journal"]
        (root / journal["staged"][OKX_ID]["staged_path"]).unlink()
        with pytest.raises(PatternLabDataError) as failure:
            recover(root, exchange, clock)
        assert failure.value.error_code == "unrecoverable_staged_artifact"
        assert "collect a separate new root" in str(failure.value)
        assert update_transaction.marker_path(root).is_file()
        assert pack_manifest.read_manifest(root)["revision"] == 1


class TestInitialJournalTemporary:
    def _interrupt_first_journal_write(self, monkeypatch):
        original = pack_manifest.write_text_atomic
        state = {"done": False}

        def wrapper(target, text):
            target = Path(target)
            if target.name == pack_manifest.UPDATE_MARKER_NAME and not state["done"]:
                state["done"] = True
                target.with_name(f".{target.name}.tmp").write_text(text, encoding="utf-8")
                raise Interrupted("interrupted during the very first journal write")
            return original(target, text)

        monkeypatch.setattr(pack_manifest, "write_text_atomic", wrapper)

    def test_a_crashed_first_write_is_reported_and_recoverable(
        self, tmp_path, roster, monkeypatch
    ):
        root = tmp_path / "fresh"
        exchange, clock, _, _ = build_exchange(slots=40)
        self._interrupt_first_journal_write(monkeypatch)
        with pytest.raises(Interrupted):
            run_collect(root, roster, exchange, clock)
        monkeypatch.undo()

        assert not update_transaction.marker_path(root).is_file()
        assert update_transaction.marker_temp_path(root).is_file()
        report = pack_data.inspect_pack(root)
        assert report["pending_operation"]["source"] == "temporary"

        with pytest.raises(PatternLabPendingError):
            run_collect(root, roster, exchange, clock)

        result = recover(root, exchange, clock)
        assert result["status"] == "recovered"
        assert_published_once(root, revision=1, events=1)

    def test_a_ready_pack_is_not_mistaken_for_a_new_generation(self, collected, monkeypatch):
        root, exchange, clock = collected
        self._interrupt_first_journal_write(monkeypatch)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        with pytest.raises(PatternLabPendingError, match="pending collector operation"):
            pack_data.load_slice(
                root, OKX_ID, start=utc(ANCHOR_MS), end=utc(ANCHOR_MS + 10 * STEP_MS)
            )
        assert pack_data.inspect_pack(root)["pending_operation"]["source"] == "temporary"
        recover(root, exchange, clock)
        assert_published_once(root, revision=2, events=2)

    def test_a_malformed_temporary_blocks_collect_and_is_never_deleted(self, tmp_path, roster):
        root = tmp_path / "fresh"
        root.mkdir()
        temp = update_transaction.marker_temp_path(root)
        temp.write_text('{"journal_version": 1}', encoding="utf-8")
        exchange, clock, _, _ = build_exchange(slots=40)
        with pytest.raises(PatternLabDataError, match="malformed pending-operation journal"):
            run_collect(root, roster, exchange, clock)
        assert temp.is_file()


class TestAbort:
    def test_abort_restores_a_reusable_lock_only_root(self, tmp_path, roster, monkeypatch):
        root = tmp_path / "fresh"
        exchange, clock, _, _ = build_exchange(slots=40)
        fail_before(
            monkeypatch, pack_collect, "_collect_instrument", lambda calls, *_: calls == 2
        )
        with pytest.raises(Interrupted):
            run_collect(root, roster, exchange, clock)
        monkeypatch.undo()

        result = pack_collect.abort_update(root)
        assert result["status"] == "aborted"
        assert sorted(item.name for item in root.iterdir()) == [pack_lock.LOCK_NAME]

        # The lock-only root is immediately reusable for a fresh collect.
        again = run_collect(root, roster, exchange, clock)
        assert again["status"] == "completed"
        assert_published_once(root, revision=1, events=1)

    def test_abort_leaves_an_existing_pack_byte_for_byte_unchanged(self, collected, monkeypatch):
        root, exchange, clock = collected
        before = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        fail_before(
            monkeypatch, pack_collect, "_collect_instrument", lambda calls, *_: calls == 2
        )
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()

        pack_collect.abort_update(root)
        after = {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}
        assert after == before
        assert pack_data.inspect_pack(root, verify=True)["verification_check"]["ok"] is True

    def test_abort_is_refused_once_applying_has_been_recorded(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.abort_update(root)
        assert failure.value.error_code == "abort_after_applying"
        assert "recover" in str(failure.value)
        recover(root, exchange, clock)
        assert_published_once(root, revision=2, events=2)

    def test_abort_without_a_pending_operation_is_an_error(self, collected):
        root, _, _ = collected
        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.abort_update(root)
        assert failure.value.error_code == "no_pending_operation"

    def test_abort_refuses_a_base_that_changed_outside_the_operation(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(
            monkeypatch, pack_collect, "_collect_instrument", lambda calls, *_: calls == 2
        )
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        target = root / pack_manifest.instrument_relative_file(OKX_ID)
        target.write_bytes(target.read_bytes() + b"tampered")
        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.abort_update(root)
        assert failure.value.error_code == "unexpected_target_state"


class TestRootIdentity:
    def test_a_moved_pending_root_is_refused_before_any_write(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()

        moved = root.parent / "moved"
        root.rename(moved)
        with pytest.raises(PatternLabDataError) as failure:
            pack_collect.recover_pack(moved, transport=exchange, clock=clock)
        assert failure.value.error_code == "root_moved"
        assert "Restore its original location" in str(failure.value)

        moved.rename(root)
        recover(root, exchange, clock)
        assert_published_once(root, revision=2, events=2)

    def test_a_ready_idle_pack_can_move(self, collected):
        root, _, _ = collected
        moved = root.parent / "relocated"
        root.rename(moved)
        assert pack_data.inspect_pack(moved, verify=True)["verification_check"]["ok"] is True

    def test_nothing_to_recover_is_reported_plainly(self, collected):
        root, exchange, clock = collected
        result = pack_collect.recover_pack(root, transport=exchange, clock=clock)
        assert result["status"] == "nothing_to_recover"
        assert result["pending_operation"] is None


class TestDurableWrites:
    def test_a_temporary_is_flushed_before_the_replace_and_the_directory_after(
        self, tmp_path, monkeypatch
    ):
        events: list[tuple[str, str]] = []
        original_file = pack_manifest.fsync_path
        original_dir = pack_manifest.fsync_directory
        original_replace = pack_collect.pack_data.os.replace

        monkeypatch.setattr(
            pack_manifest, "fsync_path", lambda path: (events.append(("file", Path(path).name)), original_file(path))[1]
        )
        monkeypatch.setattr(
            pack_manifest,
            "fsync_directory",
            lambda path: (events.append(("dir", Path(path).name)), original_dir(path))[1],
        )
        monkeypatch.setattr(
            pack_manifest.os,
            "replace",
            lambda source, target: (
                events.append(("replace", Path(target).name)),
                original_replace(source, target),
            )[1],
        )
        pack_manifest.write_text_atomic(tmp_path / "sample.txt", "content\n")
        assert events == [("file", ".sample.txt.tmp"), ("replace", "sample.txt"), ("dir", tmp_path.name)]
        assert (tmp_path / "sample.txt").read_text(encoding="utf-8") == "content\n"

    def test_the_parquet_writer_flushes_before_replacing(self, tmp_path, monkeypatch):
        events: list[str] = []
        original_file = pack_manifest.fsync_path
        original_dir = pack_manifest.fsync_directory
        original_replace = pack_data.os.replace
        monkeypatch.setattr(
            pack_manifest,
            "fsync_path",
            lambda path: (events.append("fsync-file"), original_file(path))[1],
        )
        monkeypatch.setattr(
            pack_manifest,
            "fsync_directory",
            lambda path: (events.append("fsync-dir"), original_dir(path))[1],
        )
        monkeypatch.setattr(
            pack_data.os,
            "replace",
            lambda source, target: (events.append("replace"), original_replace(source, target))[1],
        )
        stamps, values = synthetic_series(8)
        pack_data.write_ohlcv_file(tmp_path / "AAA_5m.parquet", stamps, values)
        assert events == ["fsync-file", "replace", "fsync-dir"]


def raw_write_journal(root: Path, journal: dict) -> None:
    """Write a journal without validation, to reproduce a corrupted marker."""
    update_transaction.marker_path(root).write_text(
        pack_manifest.dumps_json(journal) + "\n", encoding="utf-8", newline="\n"
    )


def live_bytes(root: Path) -> dict:
    """Return the published pack's bytes, excluding staging and coordination files."""
    return {
        path.relative_to(root): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and not any(part.startswith(".") for part in path.relative_to(root).parts)
    }


class TestOwnedStagingArtifacts:
    """Cleanup covers exactly this operation's own work, and nothing else."""

    def _interrupt_after_a_staged_file(self, monkeypatch):
        """Stop after one durable staged Parquet write, before its checkpoint."""
        return fail_after(monkeypatch, pack_data, "write_ohlcv_file", lambda calls, *_: calls == 1)

    def test_abort_removes_a_completed_but_uncheckpointed_file(self, collected, monkeypatch):
        root, exchange, clock = collected
        before = live_bytes(root)
        self._interrupt_after_a_staged_file(monkeypatch)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()

        journal = update_transaction.pending_state(root)["journal"]
        staging = root / journal["staging_dir"]
        orphan = sorted(item.name for item in staging.iterdir())
        assert journal["staged"] == {}  # the file exists without a checkpoint
        assert orphan  # ... and it is genuinely on disk

        aborted = pack_collect.abort_update(root)
        assert sorted(aborted["removed_artifacts"]) == orphan
        assert not staging.exists()
        assert update_transaction.pending_state(root) is None
        assert live_bytes(root) == before  # live data and base metadata are untouched

    def test_an_uncheckpointed_file_is_refetched_rather_than_trusted(self, collected, monkeypatch):
        root, exchange, clock = collected
        self._interrupt_after_a_staged_file(monkeypatch)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        exchange.requests.clear()
        recover(root, exchange, clock)
        # A name alone never certifies completed data: the instrument is downloaded again.
        assert any(path.endswith("candles") or "kline" in path for path, _ in exchange.requests)
        assert_published_once(root, revision=2, events=2)

    def test_an_unknown_hidden_file_is_preserved_and_reported(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        journal = update_transaction.pending_state(root)["journal"]
        note = root / journal["staging_dir"] / ".operator-note"
        note.write_text("unrecorded evidence", encoding="utf-8")

        with pytest.raises(PatternLabDataError, match="unexpected artifacts") as failure:
            recover(root, exchange, clock)
        assert failure.value.error_code == "unexpected_staging_artifact"
        assert note.read_text(encoding="utf-8") == "unrecorded evidence"
        assert update_transaction.pending_state(root) is not None  # the journal is retained

        # Once the unknown artifact is gone, the same operation finishes forward.
        note.unlink()
        recover(root, exchange, clock)
        assert_published_once(root, revision=2, events=2)

    def test_cleanup_refuses_a_symlink_instead_of_following_it(self, collected, monkeypatch):
        root, exchange, clock = collected
        before = live_bytes(root)
        fail_before(monkeypatch, pack_collect, "_enter_applying", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        journal = update_transaction.pending_state(root)["journal"]
        assert journal["phase"] == "staging"

        # A symlink borrowing an owned target name must not make cleanup unlink
        # the live file it points at, however plausible its name looks.
        target = root / pack_manifest.README_NAME
        link = root / journal["staging_dir"] / pack_manifest.README_NAME
        link.symlink_to(target)
        with pytest.raises(PatternLabDataError, match="unexpected artifacts") as failure:
            pack_collect.abort_update(root)
        assert failure.value.error_code == "unexpected_staging_artifact"
        assert link.is_symlink() and target.is_file()
        assert live_bytes(root) == before
        assert update_transaction.pending_state(root) is not None

        link.unlink()
        pack_collect.abort_update(root)
        assert live_bytes(root) == before

    def test_a_symlinked_staged_artifact_is_refused_before_any_replacement(
        self, collected, monkeypatch
    ):
        root, exchange, clock = collected
        before = live_bytes(root)
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        journal = update_transaction.pending_state(root)["journal"]
        staged = root / journal["staged"][OKX_ID]["staged_path"]
        staged.unlink()
        staged.symlink_to(root / pack_manifest.instrument_relative_file(OKX_ID))
        with pytest.raises(PatternLabDataError, match="resolves outside"):
            recover(root, exchange, clock)
        assert live_bytes(root) == before

    @pytest.mark.parametrize("stop_after", [1, 2, 3])
    def test_an_interrupted_target_metadata_write_is_recoverable(
        self, collected, monkeypatch, stop_after
    ):
        root, exchange, clock = collected
        before = live_bytes(root)
        fail_after(
            monkeypatch,
            update_transaction,
            "write_staged_text",
            lambda calls, *_: calls == stop_after,
        )
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        journal = update_transaction.pending_state(root)["journal"]
        assert journal["phase"] == "staging"  # applying is entered only after all of them
        assert live_bytes(root) == before

        recover(root, exchange, clock)
        assert_published_once(root, revision=2, events=2)

    def test_an_aborted_initial_collect_discards_its_uncheckpointed_file(
        self, tmp_path, roster, monkeypatch
    ):
        root = tmp_path / "fresh"
        exchange, clock, _, _ = build_exchange(slots=40)
        self._interrupt_after_a_staged_file(monkeypatch)
        with pytest.raises(Interrupted):
            run_collect(root, roster, exchange, clock)
        monkeypatch.undo()
        pack_collect.abort_update(root)
        assert sorted(item.name for item in root.iterdir()) == [pack_lock.LOCK_NAME]


class TestApplyPlanValidation:
    """The whole plan is validated before the first live replacement."""

    def _interrupted_applying(self, root, exchange, clock, monkeypatch):
        fail_before(monkeypatch, update_transaction, "apply_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        return update_transaction.pending_state(root)["journal"]

    def test_a_disagreeing_target_revision_fails_before_any_replacement(
        self, collected, monkeypatch
    ):
        root, exchange, clock = collected
        journal = self._interrupted_applying(root, exchange, clock, monkeypatch)
        before = live_bytes(root)
        # Base revision 1, journal target 99, frozen manifest 2.
        journal["target_revision"] = 99
        raw_write_journal(root, journal)

        with pytest.raises(PatternLabDataError) as failure:
            recover(root, exchange, clock)
        assert failure.value.error_code == "invalid_journal"
        assert pack_manifest.read_manifest(root)["revision"] == 1
        assert len(history_lines(root)) == 1
        assert update_transaction.marker_path(root).is_file()  # the marker is not cleared
        assert live_bytes(root) == before

    def test_a_frozen_manifest_that_disagrees_with_the_journal_publishes_nothing(
        self, collected, monkeypatch
    ):
        root, exchange, clock = collected
        journal = self._interrupted_applying(root, exchange, clock, monkeypatch)
        before = live_bytes(root)
        staged = root / journal["targets"]["manifest"]["staged_path"]
        frozen = json.loads(staged.read_text(encoding="utf-8"))
        frozen["collector"]["operation_id"] = "20200101T000000Z-0123456789ab"
        staged.write_text(pack_manifest.dumps_json(frozen) + "\n", encoding="utf-8", newline="\n")
        journal["targets"]["manifest"]["new_sha256"] = pack_manifest.file_sha256(staged)
        raw_write_journal(root, journal)

        with pytest.raises(PatternLabDataError, match="disagrees with the journal"):
            recover(root, exchange, clock)
        assert live_bytes(root) == before
        assert update_transaction.marker_path(root).is_file()

    def test_an_inconsistent_later_target_blocks_the_earlier_ones(self, collected, monkeypatch):
        root, exchange, clock = collected
        journal = self._interrupted_applying(root, exchange, clock, monkeypatch)
        before = live_bytes(root)
        # The manifest is published last, so a lost manifest artifact must stop the
        # operation before any instrument file or the README is replaced.
        (root / journal["targets"]["manifest"]["staged_path"]).unlink()
        with pytest.raises(PatternLabDataError) as failure:
            recover(root, exchange, clock)
        assert failure.value.error_code == "unrecoverable_staged_artifact"
        assert live_bytes(root) == before

    def test_a_consumed_staged_artifact_is_not_treated_as_corruption(self, collected, monkeypatch):
        root, exchange, clock = collected
        fail_before(monkeypatch, update_transaction, "cleanup_operation", lambda *_: True)
        with pytest.raises(Interrupted):
            run_update(root, exchange, clock)
        monkeypatch.undo()
        journal = update_transaction.pending_state(root)["journal"]
        staged = root / journal["targets"]["manifest"]["staged_path"]
        assert not staged.is_file()  # already renamed onto its destination
        # The already-published destination carries the same frozen bytes.
        assert update_transaction.frozen_target_text(root, journal, "manifest") == (
            (root / pack_manifest.MANIFEST_NAME).read_text(encoding="utf-8")
        )
        recover(root, exchange, clock)
        assert_published_once(root, revision=2, events=2)
