"""Closed-bar collection and recoverable updates for one Pattern Lab pack.

The four public operations are :func:`collect_pack` (a new pack from an explicit
roster), :func:`update_pack` (append a newly closed tail and optionally an
earlier prefix), :func:`recover_pack` (finish an interrupted operation) and
:func:`abort_update` (discard a staging operation, leaving the pack untouched).

Instruments are processed sequentially, with one instrument's old and new arrays
in RAM at a time, so memory is bounded by the largest series rather than by the
roster size.  Disk writes happen per completed instrument and at the transaction
metadata boundary, never per candle or page.  There is no downloader worker pool;
that is separate from M2's compute multiprocessing.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
import sys
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from . import (
    PatternLabDataError,
    PatternLabPendingError,
)
from . import data as pack_data
from . import exchange_data
from . import manifest as pack_manifest
from . import pack_lock
from . import update_transaction
from .manifest import BASE_STEP_MS, format_epoch_ms, to_epoch_ms

ROSTER_SCHEMA_VERSION = 1
DEFAULT_ROSTER_PATH = Path(__file__).resolve().parent / "configs" / "universe.json"
LATEST_CLOSED = "latest-closed"

# One hour of base bars re-fetched on both sides of existing coverage so an
# overlap comparison can detect a source discrepancy before it is published.
OVERLAP_BARS = 12
CONFLICT_SAMPLE_LIMIT = 10
GAP_SAMPLE_LIMIT = 10

ROSTER_TOP_LEVEL_KEYS = ("instruments", "schema_version", "universe")
UNIVERSE_KEYS = ("historical_membership", "notes", "selection_date", "selection_source")

_OKX_CONTRACT_RE = re.compile(r"^[A-Z0-9][A-Z0-9.]*-USDT-SWAP$")
_BYBIT_CONTRACT_RE = re.compile(r"^[A-Z0-9][A-Z0-9]*USDT$")
_SUPPORTED_PRODUCTS = {
    "OKX": (_OKX_CONTRACT_RE, "OKX USDT linear SWAP contracts named <BASE>-USDT-SWAP"),
    "BYBIT": (_BYBIT_CONTRACT_RE, "Bybit USDT linear perpetual contracts named <BASE>USDT"),
}


def stderr_progress(message: str) -> None:
    """Default diagnostics sink: stderr, so stdout stays structured JSON."""
    print(f"pattern-lab: {message}", file=sys.stderr, flush=True)


def _note(progress: Callable[[str], None] | None, message: str) -> None:
    if progress is not None:
        progress(message)


# --------------------------------------------------------------------------
# roster configuration
# --------------------------------------------------------------------------

def validate_roster_config(raw: Any, *, source: str) -> dict[str, Any]:
    """Validate the closed roster configuration and return its canonical form.

    The roster is configuration, not market data: it carries no dates, paths or
    HTTP options, and membership is never inferred from directory contents.
    """
    if not isinstance(raw, Mapping):
        raise PatternLabDataError(
            f"{source}: expected a roster object, got {type(raw).__name__}.",
            error_code="invalid_roster",
        )
    missing = sorted(set(ROSTER_TOP_LEVEL_KEYS) - set(raw))
    extra = sorted(set(raw) - set(ROSTER_TOP_LEVEL_KEYS))
    if missing or extra:
        raise PatternLabDataError(
            f"{source}: the roster document is closed; missing keys {missing}, unexpected keys {extra}.",
            error_code="invalid_roster",
        )
    version = pack_manifest.require_int(raw["schema_version"], f"{source}.schema_version")
    if version != ROSTER_SCHEMA_VERSION:
        raise PatternLabDataError(
            f"{source}.schema_version: unsupported roster version {version}; this build reads "
            f"{ROSTER_SCHEMA_VERSION}.",
            error_code="invalid_roster",
        )
    universe_raw = raw["universe"]
    if not isinstance(universe_raw, Mapping):
        raise PatternLabDataError(f"{source}.universe: expected an object.", error_code="invalid_roster")
    universe_extra = sorted(set(universe_raw) - set(UNIVERSE_KEYS))
    if universe_extra:
        raise PatternLabDataError(
            f"{source}.universe: unexpected keys {universe_extra}.", error_code="invalid_roster"
        )
    universe = pack_manifest.build_universe(
        selection_source=universe_raw.get("selection_source"),
        selection_date=universe_raw.get("selection_date"),
        historical_membership=universe_raw.get("historical_membership", "unknown"),
        notes=universe_raw.get("notes"),
    )
    roster = pack_manifest.validate_roster_entries(raw["instruments"], f"{source}.instruments")
    for entry in roster:
        pattern, description = _SUPPORTED_PRODUCTS.get(entry["venue"], (None, None))
        if pattern is None:
            raise PatternLabDataError(
                f"{source}: venue {entry['venue']!r} is not supported; v1 collects from "
                f"{sorted(_SUPPORTED_PRODUCTS)}.",
                error_code="unsupported_venue",
            )
        if not pattern.fullmatch(entry["contract"]) or entry["quote_currency"] != "USDT":
            raise PatternLabDataError(
                f"{source}: {entry['instrument_id']} is not supported; v1 allows only {description} "
                f"quoted in USDT.",
                error_code="unsupported_product",
            )
    return {
        "universe": universe,
        "roster": roster,
        "roster_sha256": pack_manifest.roster_sha256(roster),
        "source": source,
    }


def load_roster(path: Path | None = None) -> dict[str, Any]:
    """Load the tracked roster configuration, or an explicit custom one."""
    target = Path(path) if path is not None else DEFAULT_ROSTER_PATH
    return validate_roster_config(
        pack_manifest.read_json_file(target, source=str(target)), source=str(target)
    )


# --------------------------------------------------------------------------
# request resolution
# --------------------------------------------------------------------------

def _aligned_utc(value: Any, field: str) -> int:
    return pack_manifest.require_aligned(to_epoch_ms(value, field), BASE_STEP_MS, field)


def _resolve_end(
    client: exchange_data.HttpClient, venues: Sequence[str], end: Any, *, field: str = "end"
) -> dict[str, Any]:
    """Freeze one common safe closed end from the required venues' server clocks.

    An explicit end beyond that cutoff fails rather than being silently shortened,
    and ``latest-closed`` never survives into stored metadata: it resolves once,
    here, to a concrete timestamp.
    """
    samples = {venue: exchange_data.adapter_for(venue).server_time_ms(client) for venue in venues}
    cutoff = exchange_data.safe_closed_cutoff_ms(list(samples.values()))
    token = None
    if isinstance(end, str) and end.strip() == LATEST_CLOSED:
        token = LATEST_CLOSED
        end_ms = cutoff
    else:
        end_ms = _aligned_utc(end, field)
        if end_ms > cutoff:
            raise PatternLabDataError(
                f"{field}: {format_epoch_ms(end_ms)} is beyond the safe closed cutoff "
                f"{format_epoch_ms(cutoff)} derived from the venue server clocks; a requested end is "
                "never silently shortened.",
                error_code="end_not_closed",
            )
    return {
        "server_times_ms": {venue: int(value) for venue, value in sorted(samples.items())},
        "observed_utc": pack_manifest.format_utc(client.clock.now_utc(), "observed_utc"),
        "publication_lag_ms": exchange_data.CLOSURE_LAG_MS,
        "safe_cutoff_ms": int(cutoff),
        "resolved_end_ms": int(end_ms),
        "requested_end_token": token,
    }


# --------------------------------------------------------------------------
# fetch ranges, merging and coverage facts
# --------------------------------------------------------------------------

def fetch_ranges(
    *, effective_start_ms: int, end_ms: int, first_ms: int | None, last_ms: int | None
) -> list[tuple[int, int]]:
    """Return the coalesced half-open intervals this operation must download.

    A fresh instrument fetches the whole request.  An existing one fetches the
    missing prefix plus one hour inside existing coverage, and the new tail with
    one hour of overlap.  A later start never trims stored data and an older end
    never truncates the tail: both are simply already covered, and no inverted
    range is manufactured.
    """
    if first_ms is None or last_ms is None:
        return [(effective_start_ms, end_ms)]
    overlap = OVERLAP_BARS * BASE_STEP_MS
    ranges: list[tuple[int, int]] = []
    if effective_start_ms < first_ms:
        ranges.append((effective_start_ms, min(first_ms + overlap, end_ms)))
    tail_start = max(effective_start_ms, last_ms + BASE_STEP_MS - overlap)
    if tail_start < end_ms:
        ranges.append((tail_start, end_ms))
    ranges = [item for item in ranges if item[0] < item[1]]
    ranges.sort()
    coalesced: list[tuple[int, int]] = []
    for low, high in ranges:
        if coalesced and low <= coalesced[-1][1]:
            coalesced[-1] = (coalesced[-1][0], max(coalesced[-1][1], high))
        else:
            coalesced.append((low, high))
    return coalesced


def merge_series(
    existing_stamps: np.ndarray,
    existing_values: np.ndarray,
    fetched_stamps: np.ndarray,
    fetched_values: np.ndarray,
) -> dict[str, Any]:
    """Merge a downloaded batch into an existing series without repairing history.

    Duplicate timestamps are compared by all five canonical float64 values with
    signed zero normalized: there is no tolerance, no dedup-last and no
    averaging.  Identical overlaps preserve the existing row; any differing row
    is a conflict.  Newly observed timestamps are added, including holes inside
    the explicitly fetched ranges, and are reported by position.
    """
    if existing_stamps.size == 0:
        stamps = np.ascontiguousarray(fetched_stamps, dtype=np.int64)
        values = np.ascontiguousarray(fetched_values, dtype=np.float64)
        return {
            "stamps": stamps,
            "values": values,
            "conflicts": [],
            "conflict_count": 0,
            "prefix_rows": int(stamps.size),
            "inserted_rows": 0,
            "appended_rows": 0,
        }

    known = np.isin(fetched_stamps, existing_stamps)
    conflicts: list[dict[str, Any]] = []
    conflict_count = 0
    if known.any():
        positions = np.searchsorted(existing_stamps, fetched_stamps[known])
        differs = ~np.all(existing_values[positions] == fetched_values[known], axis=1)
        conflict_count = int(np.count_nonzero(differs))
        for index in np.flatnonzero(differs)[:CONFLICT_SAMPLE_LIMIT]:
            stamp = int(fetched_stamps[known][index])
            conflicts.append(
                {
                    "timestamp_utc": format_epoch_ms(stamp),
                    "stored": [float(item) for item in existing_values[positions[index]]],
                    "source": [float(item) for item in fetched_values[known][index]],
                }
            )

    fresh_stamps = fetched_stamps[~known]
    fresh_values = fetched_values[~known]
    first, last = int(existing_stamps[0]), int(existing_stamps[-1])
    prefix = int(np.count_nonzero(fresh_stamps < first))
    appended = int(np.count_nonzero(fresh_stamps > last))
    inserted = int(fresh_stamps.size) - prefix - appended

    stamps = np.concatenate((existing_stamps, fresh_stamps))
    values = np.concatenate((existing_values, fresh_values))
    order = np.argsort(stamps, kind="stable")
    return {
        "stamps": np.ascontiguousarray(stamps[order], dtype=np.int64),
        "values": np.ascontiguousarray(values[order], dtype=np.float64),
        "conflicts": conflicts,
        "conflict_count": conflict_count,
        "prefix_rows": prefix,
        "inserted_rows": inserted,
        "appended_rows": appended,
    }


def gap_report(stamps: np.ndarray) -> dict[str, Any]:
    """Count absent 5m slots and return a bounded sample of their ranges."""
    if stamps.size < 2:
        return {"gap_bar_count": 0, "gap_range_count": 0, "gap_ranges": []}
    steps = np.diff(stamps)
    breaks = np.flatnonzero(steps > BASE_STEP_MS)
    samples = [
        {
            "from_utc": format_epoch_ms(int(stamps[index]) + BASE_STEP_MS),
            "to_utc": format_epoch_ms(int(stamps[index + 1])),
            "missing_bars": int((int(steps[index]) - BASE_STEP_MS) // BASE_STEP_MS),
        }
        for index in breaks[:GAP_SAMPLE_LIMIT]
    ]
    return {
        "gap_bar_count": int(pack_data.missing_bar_count(stamps)),
        "gap_range_count": int(breaks.size),
        "gap_ranges": samples,
    }


def _conflict_error(instrument_id: str, merged: Mapping[str, Any]) -> PatternLabDataError:
    lines = [
        f"{instrument_id}: the source returned {merged['conflict_count']} historical value(s) that "
        "differ from the stored pack; no live file was changed.",
    ]
    for sample in merged["conflicts"]:
        lines.append(
            f"  {sample['timestamp_utc']}: stored {sample['stored']} versus source {sample['source']}"
        )
    lines.append(
        "Abort the staging operation with 'abort-update' to unblock the unchanged pack, then "
        "investigate the source discrepancy. If it persists, collect a separate new root or wait for "
        "an explicitly reviewed repair operation. There is no force or overwrite-history switch: never "
        "delete the marker or change the overlap merely to conceal the conflict."
    )
    return PatternLabDataError("\n".join(lines), error_code="historical_conflict")


# --------------------------------------------------------------------------
# provenance built from actual observations
# --------------------------------------------------------------------------

def _source_metadata(adapter, entry: Mapping[str, Any], *, observed_utc: str, operation_id: str):
    return pack_manifest.build_source_metadata(
        input_format="exchange_rest_json",
        input_dtype="float64",
        source_reference=f"{adapter.candle_reference} {entry['contract']}",
        volume_unit_evidence=adapter.quote_volume_evidence,
        extra={
            "venue": adapter.venue,
            "collector": "tools.pattern_lab.collect",
            "operation_id": operation_id,
            "observed_utc": observed_utc,
            "evidence_source": adapter.candle_reference,
        },
    )


def _verification(adapter, *, resolved_end_ms: int, closure: Mapping[str, Any]):
    flag = " OKX additionally requires the candle confirm flag '1'." if adapter.venue == "OKX" else ""
    return pack_manifest.build_verification(
        volume_quote_verified=True,
        volume_quote_evidence=adapter.quote_volume_evidence,
        closed_before_utc=format_epoch_ms(resolved_end_ms),
        closure_evidence=(
            "Only rows whose open plus 5m is at or before the frozen cutoff were retained."
            + flag
        ),
        closure_source=(
            f"{adapter.time_reference} sampled at operation start "
            f"({closure['observed_utc']}), minus a {closure['publication_lag_ms']}ms publication "
            "allowance, floored to the 5m grid."
        ),
    )


def _rules_changed(old: Any, new: Mapping[str, Any]) -> bool:
    """Compare rule objects ignoring the refreshed observation time alone."""
    if not isinstance(old, Mapping):
        return True
    return {key: value for key, value in old.items() if key != "as_of_utc"} != {
        key: value for key, value in new.items() if key != "as_of_utc"
    }


# --------------------------------------------------------------------------
# operation plumbing
# --------------------------------------------------------------------------

def _client_from_options(
    options: Mapping[str, Any],
    *,
    transport: Callable[..., Any] | None,
    clock: Any | None,
) -> exchange_data.HttpClient:
    """Build the paced HTTP client from the options frozen in the journal."""
    return exchange_data.HttpClient(
        transport=transport,
        clock=clock,
        timeout=float(options["timeout_seconds"]),
        max_attempts=int(options["max_attempts"]),
        rates={"OKX": float(options["okx_rps"]), "BYBIT": float(options["bybit_rps"])},
    )


def build_options(
    *,
    okx_rps: Any = exchange_data.DEFAULT_REQUESTS_PER_SECOND,
    bybit_rps: Any = exchange_data.DEFAULT_REQUESTS_PER_SECOND,
    timeout_seconds: float = exchange_data.DEFAULT_TIMEOUT_SECONDS,
    max_attempts: int = exchange_data.MAX_ATTEMPTS,
) -> dict[str, Any]:
    """Validate and freeze the per-operation HTTP options."""
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
        raise PatternLabDataError(
            f"timeout_seconds: expected a number, got {type(timeout_seconds).__name__}."
        )
    timeout = float(timeout_seconds)
    if not (timeout > 0.0) or timeout != timeout or timeout == float("inf"):
        raise PatternLabDataError(
            f"timeout_seconds: must be a positive finite number, got {timeout_seconds!r}."
        )
    return {
        "okx_rps": exchange_data.normalize_rps(okx_rps, "okx_rps"),
        "bybit_rps": exchange_data.normalize_rps(bybit_rps, "bybit_rps"),
        "timeout_seconds": timeout,
        "max_attempts": pack_manifest.require_int(max_attempts, "max_attempts", minimum=1),
    }


def _require_no_pending(data_root: Path, action: str) -> None:
    state = update_transaction.pending_state(data_root)
    if state is None:
        return
    if not state["valid"]:
        raise PatternLabDataError(
            update_transaction.pending_problem(state), error_code="invalid_journal"
        )
    raise PatternLabPendingError(
        f"{action} is refused: " + update_transaction.pending_problem(state)
    )


def _guard_identity(guard: pack_lock.PackGuard, journal: Mapping[str, Any]) -> None:
    """Refuse to write into a pending root that was moved away from its journal."""
    if journal["root"] != guard.identity:
        raise PatternLabDataError(
            f"{guard.root}: this pending operation was journalled for {journal['root']!r} but the root "
            f"now resolves to {guard.identity!r}. Restore its original location to recover, or preserve "
            "it and collect into a new root; never edit the journal's recorded path.",
            error_code="root_moved",
        )
    guard.verify_root_unchanged()


def _read_existing_series(data_root: Path, entry: Mapping[str, Any]):
    """Read one stored instrument, refusing an update of unknown extra columns."""
    path = pack_manifest.resolve_pack_path(data_root, entry["file"], f"{entry['instrument_id']}.file")
    columns = pack_data.parquet_column_names(path)
    unknown = [name for name in columns if name not in pack_data.PARQUET_COLUMNS]
    if unknown:
        raise PatternLabDataError(
            f"{path}: the stored file carries extra columns {unknown}. The six-column writer would "
            "silently discard them, so an update of this file is refused; reading it remains supported.",
            error_code="unsupported_extra_columns",
        )
    return pack_data.read_ohlcv_rows(path)


def _verify_base_pack(data_root: Path, manifest: Mapping[str, Any]) -> None:
    """Verify every declared file's integrity before staging an update."""
    problems: list[str] = []
    for entry in manifest["instruments"]:
        problems.extend(pack_data._verify_instrument(data_root, entry))
    if problems:
        raise PatternLabDataError(
            "the existing pack failed verification, so it is not a safe base for an update:\n  "
            + "\n  ".join(problems),
            error_code="base_integrity",
        )


# --------------------------------------------------------------------------
# preflight
# --------------------------------------------------------------------------

def _preflight(
    client: exchange_data.HttpClient,
    roster: Sequence[Mapping[str, Any]],
    *,
    starts_ms: Mapping[str, int],
    probe_ids: Sequence[str],
    progress: Callable[[str], None] | None,
) -> dict[str, Any]:
    """Check identity, rules, status and listing for EVERY roster entry first.

    Metadata alone cannot prove candle retention depth, so an initial collection
    or an earlier-start extension also makes one bounded start-boundary probe per
    otherwise eligible instrument.  Every detected failure is reported before any
    bulk download begins; probe success is never a promise that a later request
    cannot fail.
    """
    evidence: dict[str, Any] = {}
    failures: list[str] = []
    probing = set(probe_ids)

    # First pass: identity, rules, status and listing for EVERY roster entry.
    for entry in roster:
        instrument_id = entry["instrument_id"]
        adapter = exchange_data.adapter_for(entry["venue"])
        _note(progress, f"preflight metadata {instrument_id}")
        try:
            rules = adapter.instrument_metadata(client, entry["contract"])
        except PatternLabDataError as exc:
            failures.append(f"{instrument_id}: {exc}")
            continue
        start_ms = starts_ms[instrument_id]
        listed = rules["listed_at_utc"]
        if listed is not None and to_epoch_ms(listed, f"{instrument_id}.listed_at_utc") > start_ms:
            failures.append(
                f"{instrument_id}: the venue lists this contract at {listed}, after the requested "
                f"managed history start {format_epoch_ms(start_ms)}."
            )
            continue
        evidence[instrument_id] = {
            "rules": rules,
            "probe": {
                "checked": False,
                "slot_utc": format_epoch_ms(start_ms),
                "available": None,
                "reason": "the requested start is already verified in the stored pack",
            },
            "listing_known": listed is not None,
            "listed_at_utc": listed,
        }

    # Second pass: one bounded start-boundary probe per otherwise eligible entry,
    # because metadata alone cannot prove candle retention depth.
    for entry in roster:
        instrument_id = entry["instrument_id"]
        if instrument_id not in evidence or instrument_id not in probing:
            continue
        adapter = exchange_data.adapter_for(entry["venue"])
        start_ms = starts_ms[instrument_id]
        _note(progress, f"preflight start probe {instrument_id}")
        try:
            available = exchange_data.probe_first_slot(adapter, client, entry["contract"], start_ms)
        except PatternLabDataError as exc:
            failures.append(f"{instrument_id}: start-boundary probe failed: {exc}")
            evidence.pop(instrument_id)
            continue
        evidence[instrument_id]["probe"] = {
            "checked": True,
            "slot_utc": format_epoch_ms(start_ms),
            "available": bool(available),
            "reason": "one bounded page ending just after the requested first slot",
        }
        if not available:
            failures.append(
                f"{instrument_id}: the requested first slot {format_epoch_ms(start_ms)} is not "
                "retrievable; the venue's candle retention or listing does not reach it. Listing "
                "metadata alone never proves retention depth."
            )
            evidence.pop(instrument_id)
    if failures:
        raise PatternLabDataError(
            "preflight rejected the request before any bulk download; no instrument may be silently "
            "omitted or substituted:\n  " + "\n  ".join(failures),
            error_code="preflight_failed",
        )
    return evidence


# --------------------------------------------------------------------------
# per-instrument staging
# --------------------------------------------------------------------------

def _download(
    client: exchange_data.HttpClient,
    adapter,
    contract: str,
    ranges: Sequence[tuple[int, int]],
    progress: Callable[[str], None] | None,
):
    """Download every requested range for one instrument and validate the rows."""
    stamp_parts: list[np.ndarray] = []
    value_parts: list[np.ndarray] = []
    for low, high in ranges:
        _note(progress, f"{contract}: [{format_epoch_ms(low)}, {format_epoch_ms(high)})")
        stamps, values = adapter.fetch_candles(
            client, contract, start_ms=low, end_ms=high, progress=progress
        )
        stamp_parts.append(stamps)
        value_parts.append(values)
    if not stamp_parts:
        return np.zeros(0, dtype=np.int64), np.zeros((0, 5), dtype=np.float64)
    stamps = np.concatenate(stamp_parts)
    values = np.concatenate(value_parts)
    order = np.argsort(stamps, kind="stable")
    return pack_data.validate_consumed_rows(stamps[order], values[order], where=f"download {contract}")


def _staged_artifact_ok(data_root: Path, journal: Mapping[str, Any], record: Mapping[str, Any]) -> bool:
    """Return whether a checkpointed instrument can be reused instead of refetched."""
    if not record["changed"]:
        return True
    path = update_transaction.resolve_staged(
        data_root, record["staged_path"], journal["operation_id"], "staged artifact"
    )
    return path.is_file() and pack_manifest.file_sha256(path) == record["new_sha256"]


def _collect_instrument(
    data_root: Path,
    journal: Mapping[str, Any],
    entry: Mapping[str, Any],
    *,
    client: exchange_data.HttpClient,
    progress: Callable[[str], None] | None,
    base_manifest: Mapping[str, Any] | None,
    preflight: Mapping[str, Any],
    effective_start_ms: int,
    end_ms: int,
) -> dict[str, Any]:
    """Fetch, merge and stage one instrument; live files stay untouched here."""
    instrument_id = entry["instrument_id"]
    adapter = exchange_data.adapter_for(entry["venue"])
    operation_id = journal["operation_id"]

    old_entry = None if base_manifest is None else pack_manifest.find_instrument(base_manifest, instrument_id)
    if old_entry is None:
        existing_stamps = np.zeros(0, dtype=np.int64)
        existing_values = np.zeros((0, 5), dtype=np.float64)
        old_digest = None
    else:
        existing_stamps, existing_values = _read_existing_series(data_root, old_entry)
        old_digest = old_entry["sha256"]

    first_ms = int(existing_stamps[0]) if existing_stamps.size else None
    last_ms = int(existing_stamps[-1]) if existing_stamps.size else None
    ranges = fetch_ranges(
        effective_start_ms=effective_start_ms, end_ms=end_ms, first_ms=first_ms, last_ms=last_ms
    )
    fetched_stamps, fetched_values = _download(client, adapter, entry["contract"], ranges, progress)
    observed_utc = pack_manifest.format_utc(client.clock.now_utc(), "observed_utc")

    merged = merge_series(existing_stamps, existing_values, fetched_stamps, fetched_values)
    if merged["conflict_count"]:
        raise _conflict_error(instrument_id, merged)
    stamps, values = merged["stamps"], merged["values"]
    if stamps.size == 0 or int(stamps[0]) != effective_start_ms:
        observed = "an empty series" if stamps.size == 0 else format_epoch_ms(int(stamps[0]))
        raise PatternLabDataError(
            f"{instrument_id}: the requested first slot {format_epoch_ms(effective_start_ms)} is absent "
            f"after merging (the merged series starts at {observed}). The whole publication is blocked: "
            "no date is shifted, no member is omitted and no venue is substituted.",
            error_code="missing_start_coverage",
        )

    rules_now = preflight["rules"]
    old_rules = None if old_entry is None else old_entry.get("instrument_rules")
    reuse_rules = old_rules is not None and not _rules_changed(old_rules, rules_now)
    rules = old_rules if reuse_rules else rules_now

    data_changed = not (
        existing_stamps.size == stamps.size
        and np.array_equal(existing_stamps, stamps)
        and np.array_equal(existing_values, values)
    )
    staged_path = None
    if data_changed:
        directory = update_transaction.ensure_staging_dir(data_root, operation_id)
        file_name = pack_manifest.instrument_file_name(instrument_id)
        written = pack_data.write_ohlcv_file(
            directory / file_name, stamps, values, replace_existing=True, where=instrument_id
        )
        staged_path = f"{update_transaction.staging_dir_name(operation_id)}/{file_name}"
        new_digest = written.sha256
        new_entry = pack_manifest.build_instrument_entry(
            symbol=entry["symbol"],
            venue=entry["venue"],
            contract=entry["contract"],
            quote_currency=entry["quote_currency"],
            roles=entry["roles"],
            row_count=written.row_count,
            first_open_ms=written.first_open_ms,
            last_open_ms=written.last_open_ms,
            missing_bar_count=written.missing_bar_count,
            sha256=written.sha256,
            source=_source_metadata(
                adapter, entry, observed_utc=observed_utc, operation_id=operation_id
            ),
            verification=_verification(
                adapter, resolved_end_ms=end_ms, closure=journal["closure"]
            ),
            instrument_rules=rules,
        )
    else:
        new_digest = old_digest
        new_entry = dict(old_entry)
        if not reuse_rules:
            new_entry["instrument_rules"] = rules

    coverage_end_ms = int(stamps[-1]) + BASE_STEP_MS
    facts = {
        "changed": data_changed,
        "rules_changed": not reuse_rules and old_rules is not None,
        "row_count": int(stamps.size),
        "first_open_utc": format_epoch_ms(int(stamps[0])),
        "last_open_utc": format_epoch_ms(int(stamps[-1])),
        "coverage_end_utc": format_epoch_ms(coverage_end_ms),
        "missing_bar_count": int(pack_data.missing_bar_count(stamps)),
        "added_rows": merged["prefix_rows"] + merged["inserted_rows"] + merged["appended_rows"],
        "prefix_rows": merged["prefix_rows"],
        "inserted_rows": merged["inserted_rows"],
        "appended_rows": merged["appended_rows"],
        "fetched_rows": int(fetched_stamps.size),
        "fetch_ranges": [
            {"start_utc": format_epoch_ms(low), "end_utc": format_epoch_ms(high)} for low, high in ranges
        ],
        "tail_shortfall_bars": max(0, (end_ms - coverage_end_ms) // BASE_STEP_MS),
        **gap_report(stamps),
    }
    return {
        "instrument_id": instrument_id,
        "final_path": pack_manifest.instrument_relative_file(instrument_id),
        "staged_path": staged_path,
        "old_sha256": old_digest,
        "new_sha256": new_digest,
        "changed": data_changed,
        "entry": new_entry,
        "facts": facts,
    }


# --------------------------------------------------------------------------
# staging, applying and results
# --------------------------------------------------------------------------

def _probe_ids(
    roster: Sequence[Mapping[str, Any]],
    base_manifest: Mapping[str, Any] | None,
    effective_start_ms: int,
) -> list[str]:
    """Return the instruments whose requested start is not already stored."""
    if base_manifest is None:
        return [entry["instrument_id"] for entry in roster]
    probing: list[str] = []
    for entry in roster:
        stored = pack_manifest.find_instrument(base_manifest, entry["instrument_id"])
        if to_epoch_ms(stored["first_open_utc"], "first_open_utc") > effective_start_ms:
            probing.append(entry["instrument_id"])
    return probing


def _stage(
    data_root: Path,
    guard: pack_lock.PackGuard,
    journal: Mapping[str, Any],
    *,
    client: exchange_data.HttpClient,
    progress: Callable[[str], None] | None,
    base_manifest: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Run the staging phase, reusing every completed checkpoint by digest."""
    journal = dict(journal)
    effective_start_ms = to_epoch_ms(journal["request"]["start_utc"], "request.start_utc")
    end_ms = to_epoch_ms(journal["request"]["end_utc"], "request.end_utc")
    roster = journal["roster"]

    preflight = dict(journal["preflight"])
    if any(entry["instrument_id"] not in preflight for entry in roster):
        preflight = _preflight(
            client,
            roster,
            starts_ms={entry["instrument_id"]: effective_start_ms for entry in roster},
            probe_ids=_probe_ids(roster, base_manifest, effective_start_ms),
            progress=progress,
        )
        journal["preflight"] = preflight
        journal = update_transaction.write_journal(data_root, journal)

    staged = dict(journal["staged"])
    for index, entry in enumerate(roster, start=1):
        instrument_id = entry["instrument_id"]
        if instrument_id in staged and _staged_artifact_ok(data_root, journal, staged[instrument_id]):
            _note(progress, f"[{index}/{len(roster)}] {instrument_id}: reusing the completed stage")
            continue
        _note(progress, f"[{index}/{len(roster)}] {instrument_id}: collecting")
        staged[instrument_id] = _collect_instrument(
            data_root,
            journal,
            entry,
            client=client,
            progress=progress,
            base_manifest=base_manifest,
            preflight=preflight[instrument_id],
            effective_start_ms=effective_start_ms,
            end_ms=end_ms,
        )
        journal["staged"] = staged
        journal = update_transaction.write_journal(data_root, journal)
        guard.verify_root_unchanged()
    journal["staged"] = staged
    return journal


def _is_no_op(journal: Mapping[str, Any], base_manifest: Mapping[str, Any] | None) -> bool:
    """A no-op changed no rows and no relevant metadata; only the check time moved."""
    if base_manifest is None:
        return False
    for entry in journal["roster"]:
        record = journal["staged"][entry["instrument_id"]]
        if record["changed"]:
            return False
        stored = pack_manifest.find_instrument(base_manifest, entry["instrument_id"])
        if record["entry"] != stored:
            return False
    return True


def _update_source_record(journal: Mapping[str, Any]) -> dict[str, Any]:
    """Build the deterministic history-event source block for this operation."""
    shortfalls = {
        instrument_id: record["facts"]["tail_shortfall_bars"]
        for instrument_id, record in sorted(journal["staged"].items())
        if record["facts"]["tail_shortfall_bars"]
    }
    return {
        "kind": "pattern_lab_collector",
        "operation": journal["kind"],
        "operation_id": journal["operation_id"],
        "requested_start_utc": journal["request"]["start_utc"],
        "requested_end_utc": journal["request"]["end_utc"],
        "requested_end_token": journal["request"]["requested_end_token"],
        "roster_sha256": journal["roster_sha256"],
        "changed_instruments": sorted(
            instrument_id
            for instrument_id, record in journal["staged"].items()
            if record["changed"]
        ),
        "added_rows": sum(record["facts"]["added_rows"] for record in journal["staged"].values()),
        "inserted_historical_rows": sum(
            record["facts"]["inserted_rows"] for record in journal["staged"].values()
        ),
        "tail_shortfall_bars": shortfalls,
    }


def _enter_applying(
    data_root: Path, journal: Mapping[str, Any], progress: Callable[[str], None] | None
) -> dict[str, Any]:
    """Freeze every final metadata byte, then record the applying phase durably."""
    journal = dict(journal)
    operation_id = journal["operation_id"]
    entries = [journal["staged"][entry["instrument_id"]]["entry"] for entry in journal["roster"]]
    collector = {
        "schema_version": pack_manifest.COLLECTOR_SCHEMA_VERSION,
        "roster": journal["roster"],
        "roster_sha256": journal["roster_sha256"],
        "managed_start_utc": journal["request"]["start_utc"],
        "last_request": {
            "start_utc": journal["request"]["start_utc"],
            "end_utc": journal["request"]["end_utc"],
        },
        "operation_id": operation_id,
    }
    manifest = pack_manifest.build_manifest(
        instruments=entries,
        universe=journal["universe"],
        generated_utc=journal["operation_started_utc"],
        revision=journal["target_revision"],
        state="ready",
        collector=collector,
    )
    record = pack_manifest.build_update_record(
        manifest,
        event=journal["kind"],
        source=_update_source_record(journal),
        note=journal["request"].get("note"),
    )
    history_path = Path(data_root) / pack_manifest.UPDATES_NAME
    history = history_path.read_text(encoding="utf-8") if history_path.is_file() else ""

    _note(progress, "staging the target manifest, README and history")
    targets = {
        "updates": update_transaction.write_staged_text(
            data_root, operation_id, pack_manifest.UPDATES_NAME,
            history + pack_manifest.render_update_line(record),
        ),
        "readme": update_transaction.write_staged_text(
            data_root, operation_id, pack_manifest.README_NAME, pack_manifest.render_readme(manifest)
        ),
        "manifest": update_transaction.write_staged_text(
            data_root, operation_id, pack_manifest.MANIFEST_NAME,
            pack_manifest.dumps_json(manifest) + "\n",
        ),
    }
    base = journal["base"]
    targets["updates"]["old_sha256"] = base["updates_sha256"]
    targets["readme"]["old_sha256"] = base["readme_sha256"]
    targets["manifest"]["old_sha256"] = base["manifest_sha256"]
    journal["targets"] = targets
    journal["phase"] = "applying"
    return update_transaction.write_journal(data_root, journal)


def _result(
    data_root: Path,
    journal: Mapping[str, Any],
    *,
    status: str,
    revision_before: int | None,
    revision_after: int | None,
    changed_files: Sequence[str],
    request_count: int,
) -> dict[str, Any]:
    """Return the structured operation result printed to stdout."""
    instruments = []
    shortfall_total = 0
    for entry in journal["roster"]:
        record = journal["staged"][entry["instrument_id"]]
        facts = dict(record["facts"])
        shortfall_total += facts["tail_shortfall_bars"]
        instruments.append({"instrument_id": entry["instrument_id"], "roles": entry["roles"], **facts})
    return {
        "operation_id": journal["operation_id"],
        "kind": journal["kind"],
        "status": status,
        "data_root": str(Path(data_root)),
        "resolved_request": {
            "start_utc": journal["request"]["start_utc"],
            "end_utc": journal["request"]["end_utc"],
            "requested_end_token": journal["request"]["requested_end_token"],
            "requested_start_utc": journal["request"].get("requested_start_utc"),
        },
        "closure": dict(journal["closure"]),
        "options": dict(journal["options"]),
        "revision_before": revision_before,
        "revision_after": revision_after,
        "instrument_count": len(journal["roster"]),
        "instruments": instruments,
        "changed_files": sorted(changed_files),
        "added_rows": sum(item["added_rows"] for item in instruments),
        "inserted_historical_rows": sum(item["inserted_rows"] for item in instruments),
        "tail_shortfall_bars": shortfall_total,
        "instruments_with_tail_shortfall": sorted(
            item["instrument_id"] for item in instruments if item["tail_shortfall_bars"]
        ),
        "http_request_count": request_count,
    }


def _execute(
    data_root: Path,
    guard: pack_lock.PackGuard,
    journal: Mapping[str, Any],
    *,
    client: exchange_data.HttpClient,
    progress: Callable[[str], None] | None,
    base_manifest: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Finish one operation from its current phase and return the result."""
    revision_before = journal["base"]["revision"]
    if journal["phase"] == "staging":
        journal = _stage(
            data_root, guard, journal, client=client, progress=progress, base_manifest=base_manifest
        )
        if _is_no_op(journal, base_manifest):
            # Nothing changed: preserve every byte, publish no revision and record
            # no history event. Only the command's own result carries the new
            # checking time.
            _note(progress, "no new closed rows and no rule changes: preserving the pack unchanged")
            update_transaction.verify_base_intact(data_root, journal)
            update_transaction.cleanup_operation(data_root, journal)
            update_transaction.remove_marker(data_root)
            result = _result(
                data_root,
                journal,
                status="no_op",
                revision_before=revision_before,
                revision_after=revision_before,
                changed_files=[],
                request_count=client.request_count,
            )
            result["checked_utc"] = pack_manifest.format_utc(client.clock.now_utc(), "checked_utc")
            return result
        journal = _enter_applying(data_root, journal, progress)
    applied = update_transaction.apply_operation(data_root, journal, progress=progress)
    return _result(
        data_root,
        journal,
        status="completed",
        revision_before=revision_before,
        revision_after=journal["target_revision"],
        changed_files=applied["replaced_files"],
        request_count=client.request_count,
    )


# --------------------------------------------------------------------------
# public operations
# --------------------------------------------------------------------------

def _base_digests(data_root: Path, manifest: Mapping[str, Any] | None) -> dict[str, Any]:
    """Record the exact base the operation is allowed to replace."""
    root = Path(data_root)
    if manifest is None:
        return {
            "revision": None,
            "manifest_sha256": None,
            "readme_sha256": None,
            "updates_sha256": None,
            "files": {},
        }
    return {
        "revision": manifest["revision"],
        "manifest_sha256": update_transaction.digest_or_none(root / pack_manifest.MANIFEST_NAME),
        "readme_sha256": update_transaction.digest_or_none(root / pack_manifest.README_NAME),
        "updates_sha256": update_transaction.digest_or_none(root / pack_manifest.UPDATES_NAME),
        "files": {entry["instrument_id"]: entry["sha256"] for entry in manifest["instruments"]},
    }


def _new_journal(
    *,
    kind: str,
    guard: pack_lock.PackGuard,
    operation_id: str,
    started: datetime,
    roster_config: Mapping[str, Any],
    request: Mapping[str, Any],
    options: Mapping[str, Any],
    closure: Mapping[str, Any],
    base: Mapping[str, Any],
    target_revision: int,
) -> dict[str, Any]:
    return {
        "journal_version": update_transaction.JOURNAL_VERSION,
        "operation_id": operation_id,
        "kind": kind,
        "root": guard.identity,
        "staging_dir": update_transaction.staging_dir_name(operation_id),
        "operation_started_utc": pack_manifest.format_utc(started, "operation_started_utc"),
        "request": dict(request),
        "options": dict(options),
        "universe": dict(roster_config["universe"]),
        "roster": [dict(entry) for entry in roster_config["roster"]],
        "roster_sha256": roster_config["roster_sha256"],
        "base": dict(base),
        "target_revision": target_revision,
        "phase": "staging",
        "closure": dict(closure),
        "preflight": {},
        "staged": {},
        "targets": None,
    }


def _require_collect_destination(data_root: Path) -> None:
    """A new collect needs an absent, empty or lock-only destination."""
    root = Path(data_root)
    if not root.exists():
        return
    if not root.is_dir():
        raise PatternLabDataError(
            f"{root}: the destination exists and is not a directory.", error_code="destination_not_empty"
        )
    _require_no_pending(root, "collect")
    unexpected = update_transaction.unexpected_root_artifacts(root, [pack_lock.LOCK_NAME])
    if unexpected:
        raise PatternLabDataError(
            f"{root}: a new collect needs an absent, empty or lock-only destination, but it contains "
            f"{unexpected}. Use 'update' for an existing collector-managed pack, 'recover' for a pending "
            "operation, or choose another root.",
            error_code="destination_not_empty",
        )


def collect_pack(
    data_root: Path,
    *,
    start: Any,
    end: Any,
    roster_path: Path | None = None,
    roster_config: Mapping[str, Any] | None = None,
    options: Mapping[str, Any] | None = None,
    transport: Callable[..., Any] | None = None,
    clock: Any | None = None,
    progress: Callable[[str], None] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Collect a brand-new pack for an explicit roster over ``[start, end)``."""
    pack_data.require_pyarrow()
    resolved = dict(roster_config) if roster_config is not None else load_roster(roster_path)
    root = Path(data_root)
    pack_lock.resolve_root_identity(root)  # fails early when the parent is missing
    _require_collect_destination(root)
    frozen = build_options(**dict(options or {}))

    with pack_lock.pack_guard(root, create_root=True) as guard:
        # Re-check admissibility under the lock: a competing initial collect that
        # created this directory first must never be overwritten.
        _require_collect_destination(root)
        client = _client_from_options(frozen, transport=transport, clock=clock)
        venues = sorted({entry["venue"] for entry in resolved["roster"]})
        closure = _resolve_end(client, venues, end)
        start_ms = _aligned_utc(start, "start")
        if start_ms >= closure["resolved_end_ms"]:
            raise PatternLabDataError(
                f"start: requires start < end, got {format_epoch_ms(start_ms)} >= "
                f"{format_epoch_ms(closure['resolved_end_ms'])}.",
                error_code="invalid_interval",
            )
        started = client.clock.now_utc()
        operation_id = update_transaction.new_operation_id(started)
        journal = _new_journal(
            kind="collect",
            guard=guard,
            operation_id=operation_id,
            started=started,
            roster_config=resolved,
            request={
                "start_utc": format_epoch_ms(start_ms),
                "end_utc": format_epoch_ms(closure["resolved_end_ms"]),
                "requested_end_token": closure["requested_end_token"],
                "requested_start_utc": format_epoch_ms(start_ms),
                "note": note,
            },
            options=frozen,
            closure=closure,
            base=_base_digests(root, None),
            target_revision=1,
        )
        journal = update_transaction.write_journal(root, journal)
        (root / pack_manifest.OHLCV_DIR).mkdir(exist_ok=True)
        _note(progress, f"operation {operation_id}: collecting {len(resolved['roster'])} instruments")
        return _execute(
            root, guard, journal, client=client, progress=progress, base_manifest=None
        )


def update_pack(
    data_root: Path,
    *,
    end: Any,
    start: Any = None,
    options: Mapping[str, Any] | None = None,
    transport: Callable[..., Any] | None = None,
    clock: Any | None = None,
    progress: Callable[[str], None] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Extend an existing collector-managed pack; the roster is never changed here."""
    pack_data.require_pyarrow()
    root = Path(data_root)
    frozen = build_options(**dict(options or {}))

    with pack_lock.pack_guard(root) as guard:
        _require_no_pending(root, "update")
        manifest = pack_manifest.read_manifest(root)
        if manifest["state"] != "ready":
            raise PatternLabDataError(
                f"{root}: manifest state is {manifest['state']!r}; only a ready pack can be updated.",
                error_code="pack_not_ready",
            )
        collector = manifest.get("collector")
        if collector is None:
            raise PatternLabDataError(
                f"{root}: this pack has no collector provenance, so it is an archival pack (for example "
                "an NPZ import) rather than a collector-managed one. It is never silently adopted as a "
                "collector transaction; collect a separate managed root instead.",
                error_code="unmanaged_pack",
            )
        _verify_base_pack(root, manifest)
        for entry in manifest["instruments"]:
            _read_existing_series(root, entry)  # rejects unknown extra OHLCV columns up front

        resolved = {
            "universe": dict(manifest["universe"]),
            "roster": [dict(item) for item in collector["roster"]],
            "roster_sha256": collector["roster_sha256"],
        }
        client = _client_from_options(frozen, transport=transport, clock=clock)
        venues = sorted({entry["venue"] for entry in resolved["roster"]})
        closure = _resolve_end(client, venues, end)
        end_ms = closure["resolved_end_ms"]
        managed_start_ms = to_epoch_ms(collector["managed_start_utc"], "collector.managed_start_utc")
        requested_start_ms = None if start is None else _aligned_utc(start, "start")
        effective_start_ms = (
            managed_start_ms
            if requested_start_ms is None
            else min(managed_start_ms, requested_start_ms)
        )
        if effective_start_ms >= end_ms:
            raise PatternLabDataError(
                f"start: requires the effective start {format_epoch_ms(effective_start_ms)} to precede "
                f"the requested end {format_epoch_ms(end_ms)}.",
                error_code="invalid_interval",
            )
        started = client.clock.now_utc()
        operation_id = update_transaction.new_operation_id(started)
        journal = _new_journal(
            kind="update",
            guard=guard,
            operation_id=operation_id,
            started=started,
            roster_config=resolved,
            request={
                "start_utc": format_epoch_ms(effective_start_ms),
                "end_utc": format_epoch_ms(end_ms),
                "requested_end_token": closure["requested_end_token"],
                "requested_start_utc": None
                if requested_start_ms is None
                else format_epoch_ms(requested_start_ms),
                "note": note,
            },
            options=frozen,
            closure=closure,
            base=_base_digests(root, manifest),
            target_revision=manifest["revision"] + 1,
        )
        journal = update_transaction.write_journal(root, journal)
        _note(progress, f"operation {operation_id}: updating revision {manifest['revision']}")
        return _execute(
            root, guard, journal, client=client, progress=progress, base_manifest=manifest
        )


def _promote_initial_journal(
    data_root: Path, guard: pack_lock.PackGuard, journal: Mapping[str, Any]
) -> dict[str, Any]:
    """Promote a crashed first journal write only after full root validation."""
    root = Path(data_root)
    if journal["kind"] == "collect":
        allowed = [
            pack_lock.LOCK_NAME,
            pack_manifest.UPDATE_MARKER_TEMP_NAME,
            journal["staging_dir"],
            pack_manifest.OHLCV_DIR,
        ]
        unexpected = update_transaction.unexpected_root_artifacts(root, allowed)
        occupied = [
            name
            for name in (journal["staging_dir"], pack_manifest.OHLCV_DIR)
            if (root / name).is_dir() and any((root / name).iterdir())
        ]
        if unexpected or occupied:
            raise PatternLabDataError(
                f"{root}: the initial journal temporary cannot be promoted because the destination is "
                f"not lock-only (unexpected {unexpected}, non-empty {occupied}). Inspect it manually.",
                error_code="invalid_journal",
            )
    else:
        update_transaction.verify_base_intact(root, journal)
    return update_transaction.write_journal(root, journal)


def recover_pack(
    data_root: Path,
    *,
    transport: Callable[..., Any] | None = None,
    clock: Any | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Finish an interrupted operation, reusing every completed staged artifact."""
    pack_data.require_pyarrow()
    root = Path(data_root)
    with pack_lock.pack_guard(root) as guard:
        state = update_transaction.pending_state(root)
        if state is None:
            return {
                "status": "nothing_to_recover",
                "data_root": str(root),
                "pending_operation": None,
            }
        if not state["valid"]:
            raise PatternLabDataError(
                update_transaction.pending_problem(state), error_code="invalid_journal"
            )
        journal = state["journal"]
        _guard_identity(guard, journal)
        if state["source"] == "temporary":
            _note(progress, "promoting the interrupted initial journal write")
            journal = _promote_initial_journal(root, guard, journal)

        base_manifest = None
        if journal["kind"] == "update" and journal["phase"] == "staging":
            update_transaction.verify_base_intact(root, journal)
            base_manifest = pack_manifest.read_manifest(root)
        if journal["kind"] == "collect" and journal["phase"] == "staging":
            (root / pack_manifest.OHLCV_DIR).mkdir(exist_ok=True)

        client = _client_from_options(journal["options"], transport=transport, clock=clock)
        _note(
            progress,
            f"recovering operation {journal['operation_id']} from the {journal['phase']} phase",
        )
        result = _execute(
            root, guard, journal, client=client, progress=progress, base_manifest=base_manifest
        )
        result["status"] = "recovered" if result["status"] == "completed" else result["status"]
        return result


def abort_update(
    data_root: Path, *, progress: Callable[[str], None] | None = None
) -> dict[str, Any]:
    """Abort a staging operation; the applying phase must be finished forward."""
    root = Path(data_root)
    with pack_lock.pack_guard(root) as guard:
        state = update_transaction.pending_state(root)
        if state is None:
            raise PatternLabDataError(
                f"{root}: there is no pending operation to abort.",
                error_code="no_pending_operation",
            )
        if not state["valid"]:
            raise PatternLabDataError(
                update_transaction.pending_problem(state), error_code="invalid_journal"
            )
        journal = state["journal"]
        _guard_identity(guard, journal)
        _note(progress, f"aborting operation {journal['operation_id']}")
        aborted = update_transaction.abort_operation(root, journal)
        removed_directories: list[str] = []
        if journal["kind"] == "collect":
            removed_directories = update_transaction.remove_empty_created_directories(
                root, [pack_manifest.OHLCV_DIR]
            )
        return {
            "status": "aborted",
            "operation_id": journal["operation_id"],
            "kind": journal["kind"],
            "data_root": str(root),
            "revision_before": journal["base"]["revision"],
            "revision_after": journal["base"]["revision"],
            "removed_artifacts": aborted["removed_artifacts"],
            "removed_directories": removed_directories,
            "retained": [pack_lock.LOCK_NAME],
        }
