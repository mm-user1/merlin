"""Durable staging, commit and recovery for one collector operation.

The existing ``.update-in-progress.json`` marker is the operation journal.  One
operation-owned staging directory under the same data root holds every completed
replacement until the whole target set is ready.  There is no whole-pack
snapshot, no general transaction engine and no backup service.

Two phases are sufficient:

``staging``
    The operation is recorded before any work.  Completed per-instrument
    artifacts are checkpointed as they are written.  Live OHLCV and the
    authoritative metadata stay untouched, so staging can always be aborted.

``applying``
    Entered durably only once every required artifact exists and verifies.  From
    there recovery only ever moves forward: changed files, then history and
    README, then the manifest last, then recorded cleanup, then the marker.

This module owns journal validation and the file mechanics.  It performs no
network access and never imports PyArrow, so a journal can be inspected in an
environment without the optional wheel.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

from . import PatternLabDataError
from . import exchange_data
from . import manifest as pack_manifest
from .manifest import (
    BASE_STEP_MS,
    MANIFEST_NAME,
    README_NAME,
    UPDATE_MARKER_NAME,
    UPDATE_MARKER_TEMP_NAME,
    UPDATES_NAME,
    file_sha256,
    format_epoch_ms,
    format_utc,
    require_aligned,
    require_aligned_utc,
    require_bool,
    require_int,
    require_optional_text,
    require_sha256,
    require_text,
    to_epoch_ms,
)

JOURNAL_VERSION = 1
STAGING_PREFIX = ".pack-staging-"
OPERATION_KINDS = ("collect", "update")
PHASES = ("staging", "applying")
TEXT_TARGETS = ("updates", "readme", "manifest")
TARGET_FINAL_NAMES = {"updates": UPDATES_NAME, "readme": README_NAME, "manifest": MANIFEST_NAME}

REQUEST_KEYS = ("end_utc", "note", "requested_end_token", "requested_start_utc", "start_utc")
CLOSURE_KEYS = (
    "observed_utc",
    "publication_lag_ms",
    "requested_end_token",
    "resolved_end_ms",
    "safe_cutoff_ms",
    "server_times_ms",
)
PREFLIGHT_KEYS = ("listed_at_utc", "listing_known", "probe", "rules")
PROBE_KEYS = ("available", "checked", "reason", "slot_utc")
FETCH_RANGE_KEYS = ("end_utc", "start_utc")
GAP_RANGE_KEYS = ("from_utc", "missing_bars", "to_utc")
# The producer's bounded gap sample size.  It lives here, beside the validator
# that checks it, so the collector can share one limit without a circular import.
GAP_SAMPLE_LIMIT = 10
FACT_BOOL_KEYS = ("changed", "rules_changed")
FACT_TEXT_KEYS = ("coverage_end_utc", "first_open_utc", "last_open_utc")
FACT_COUNT_KEYS = (
    "added_rows",
    "appended_rows",
    "fetched_rows",
    "gap_bar_count",
    "gap_range_count",
    "inserted_rows",
    "missing_bar_count",
    "prefix_rows",
    "row_count",
    "tail_shortfall_bars",
)
FACT_LIST_KEYS = ("fetch_ranges", "gap_ranges")
FACT_KEYS = FACT_BOOL_KEYS + FACT_TEXT_KEYS + FACT_COUNT_KEYS + FACT_LIST_KEYS

_OPERATION_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$")
_STAGED_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
# The Parquet writer's same-directory temporary convention, ``.<name>.tmp-<uuid>``.
_STAGED_TEMP_RE = re.compile(r"^\.(?P<base>.+)\.tmp-[0-9a-f]{32}$")


def new_operation_id(moment: datetime) -> str:
    """Return a sortable, filesystem-safe operation identifier."""
    stamp = moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid4().hex[:12]}"


def marker_path(data_root: Path) -> Path:
    return Path(data_root) / UPDATE_MARKER_NAME


def marker_temp_path(data_root: Path) -> Path:
    return Path(data_root) / UPDATE_MARKER_TEMP_NAME


def staging_dir_name(operation_id: str) -> str:
    return f"{STAGING_PREFIX}{operation_id}"


def staging_dir(data_root: Path, operation_id: str) -> Path:
    return Path(data_root) / staging_dir_name(operation_id)


# --------------------------------------------------------------------------
# journal validation
# --------------------------------------------------------------------------

def _mapping(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PatternLabDataError(
            f"{where}: expected an object, got {type(value).__name__}.", error_code="invalid_journal"
        )
    return dict(value)


def _journal_error(message: str) -> PatternLabDataError:
    return PatternLabDataError(message, error_code="invalid_journal")


def _relative_staged_path(value: Any, operation_id: str, where: str) -> str:
    """Validate a staged path: exactly one file inside this operation's directory."""
    text = require_text(value, where)
    if "\\" in text or text.startswith("/") or ":" in text:
        raise _journal_error(f"{where}: {text!r} is not a relative POSIX pack path.")
    parts = PurePosixPath(text).parts
    if len(parts) != 2 or parts[0] != staging_dir_name(operation_id):
        raise _journal_error(
            f"{where}: {text!r} must be '{staging_dir_name(operation_id)}/<name>'."
        )
    if not _STAGED_NAME_RE.fullmatch(parts[1]):
        raise _journal_error(f"{where}: unsafe staged file name {parts[1]!r}.")
    return text


def _validate_fetch_ranges(items: Sequence[Any], where: str, *, start_ms: int, end_ms: int) -> None:
    """Validate the recorded fetch ranges: aligned, nonempty, sorted and coalesced.

    An empty list is valid: an already-covered older request downloads nothing.
    These are the intervals this operation actually fetched, not the stored
    coverage a pack retains outside the effective request.
    """
    previous_end: int | None = None
    for index, item in enumerate(items):
        position = f"{where}[{index}]"
        record = _mapping(item, position)
        missing = sorted(set(FETCH_RANGE_KEYS) - set(record))
        extra = sorted(set(record) - set(FETCH_RANGE_KEYS))
        if missing or extra:
            raise _journal_error(
                f"{position}: a fetch range is closed; missing keys {missing}, unexpected keys {extra}."
            )
        try:
            low = require_aligned_utc(record["start_utc"], f"{position}.start_utc")
            high = require_aligned_utc(record["end_utc"], f"{position}.end_utc")
        except PatternLabDataError as exc:
            raise _journal_error(str(exc)) from exc
        if low >= high:
            raise _journal_error(
                f"{position}: requires start_utc < end_utc, got {format_epoch_ms(low)} >= "
                f"{format_epoch_ms(high)}."
            )
        if low < start_ms or high > end_ms:
            raise _journal_error(
                f"{position}: [{format_epoch_ms(low)}, {format_epoch_ms(high)}) falls outside the "
                f"effective request [{format_epoch_ms(start_ms)}, {format_epoch_ms(end_ms)})."
            )
        if previous_end is not None and low <= previous_end:
            raise _journal_error(
                f"{position}: fetch ranges are recorded sorted and coalesced, but "
                f"{format_epoch_ms(low)} does not follow {format_epoch_ms(previous_end)}."
            )
        previous_end = high


def _validate_gap_ranges(
    items: Sequence[Any], where: str, *, first_ms: int, last_ms: int, range_count: int, total_bars: int
) -> None:
    """Validate the bounded gap sample against the declared counts and coverage.

    The sample is the producer's first :data:`GAP_SAMPLE_LIMIT` ranges, so a
    truncated sample must stay internally valid without its missing bars adding
    up to the full total.  The positions of unsampled gaps are never inferred.
    """
    expected = min(range_count, GAP_SAMPLE_LIMIT)
    if len(items) != expected:
        raise _journal_error(
            f"{where}: {range_count} gap range(s) produce a bounded sample of {expected}, "
            f"got {len(items)}."
        )
    previous_to: int | None = None
    sampled = 0
    for index, item in enumerate(items):
        position = f"{where}[{index}]"
        record = _mapping(item, position)
        missing = sorted(set(GAP_RANGE_KEYS) - set(record))
        extra = sorted(set(record) - set(GAP_RANGE_KEYS))
        if missing or extra:
            raise _journal_error(
                f"{position}: a gap range is closed; missing keys {missing}, unexpected keys {extra}."
            )
        try:
            low = require_aligned_utc(record["from_utc"], f"{position}.from_utc")
            high = require_aligned_utc(record["to_utc"], f"{position}.to_utc")
            bars = require_int(record["missing_bars"], f"{position}.missing_bars", minimum=1)
        except PatternLabDataError as exc:
            raise _journal_error(str(exc)) from exc
        if low >= high:
            raise _journal_error(
                f"{position}: requires from_utc < to_utc, got {format_epoch_ms(low)} >= "
                f"{format_epoch_ms(high)}."
            )
        if low <= first_ms or high > last_ms:
            raise _journal_error(
                f"{position}: [{format_epoch_ms(low)}, {format_epoch_ms(high)}) is not a gap inside "
                f"the stored coverage ({format_epoch_ms(first_ms)} to {format_epoch_ms(last_ms)})."
            )
        if bars != (high - low) // BASE_STEP_MS:
            raise _journal_error(
                f"{position}.missing_bars: {bars} is not the {(high - low) // BASE_STEP_MS} slot(s) "
                f"between {format_epoch_ms(low)} and {format_epoch_ms(high)}."
            )
        if previous_to is not None and low < previous_to:
            raise _journal_error(
                f"{position}: gap ranges are recorded sorted and nonoverlapping, but "
                f"{format_epoch_ms(low)} precedes {format_epoch_ms(previous_to)}."
            )
        previous_to = high
        sampled += bars
    if sampled > total_bars:
        raise _journal_error(
            f"{where}: the sampled {sampled} missing bar(s) exceed the declared total {total_bars}."
        )
    if range_count <= GAP_SAMPLE_LIMIT and sampled != total_bars:
        raise _journal_error(
            f"{where}: the complete sample accounts for {sampled} missing bar(s), not the declared "
            f"total {total_bars}."
        )


def _validate_facts(
    raw: Any, entry: Mapping[str, Any], where: str, *, request: Mapping[str, Any], changed: bool
) -> dict[str, Any]:
    """Validate one staged record's reported facts and tie them to its entry.

    Every constraint here follows from the saved request, the staged manifest
    entry and the producer's declared formats, so contradictory counters are
    rejected at journal admission instead of becoming published history or a
    completion exit code.  Nothing is repaired and no market observation is
    invented.
    """
    facts = _mapping(raw, where)
    missing = sorted(set(FACT_KEYS) - set(facts))
    extra = sorted(set(facts) - set(FACT_KEYS))
    if missing or extra:
        raise _journal_error(
            f"{where}: the facts object is closed; missing keys {missing}, unexpected keys {extra}."
        )
    try:
        for key in FACT_BOOL_KEYS:
            require_bool(facts[key], f"{where}.{key}")
        for key in FACT_COUNT_KEYS:
            require_int(facts[key], f"{where}.{key}", minimum=0)
        for key in FACT_TEXT_KEYS:
            require_text(facts[key], f"{where}.{key}")
        for key in FACT_LIST_KEYS:
            if not isinstance(facts[key], list):
                raise PatternLabDataError(f"{where}.{key}: expected a list.")
    except PatternLabDataError as exc:
        raise _journal_error(str(exc)) from exc
    for key in ("row_count", "first_open_utc", "last_open_utc", "coverage_end_utc", "missing_bar_count"):
        if facts[key] != entry[key]:
            raise _journal_error(
                f"{where}.{key}: {facts[key]!r} does not agree with the staged manifest entry "
                f"{entry[key]!r}."
            )

    if facts["changed"] != changed:
        raise _journal_error(
            f"{where}.changed: {facts['changed']!r} does not agree with the staged record's "
            f"{changed!r}."
        )
    components = facts["prefix_rows"] + facts["inserted_rows"] + facts["appended_rows"]
    if facts["added_rows"] != components:
        raise _journal_error(
            f"{where}.added_rows: {facts['added_rows']} is not the sum {components} of its prefix, "
            "inserted and appended components."
        )
    if not changed and facts["added_rows"]:
        raise _journal_error(
            f"{where}.added_rows: {facts['added_rows']} row(s) were added although the data are "
            "recorded as unchanged; a rules-only update adds no rows."
        )
    if facts["gap_bar_count"] != facts["missing_bar_count"]:
        raise _journal_error(
            f"{where}.gap_bar_count: {facts['gap_bar_count']} does not agree with the entry's "
            f"missing_bar_count {facts['missing_bar_count']}."
        )
    if facts["gap_range_count"] > facts["gap_bar_count"]:
        raise _journal_error(
            f"{where}.gap_range_count: {facts['gap_range_count']} range(s) cannot hold only "
            f"{facts['gap_bar_count']} missing bar(s)."
        )
    if bool(facts["gap_range_count"]) != bool(facts["gap_bar_count"]):
        raise _journal_error(
            f"{where}: {facts['gap_range_count']} gap range(s) and {facts['gap_bar_count']} missing "
            "bar(s) cannot both be recorded; zero gaps have no ranges."
        )

    start_ms = to_epoch_ms(request["start_utc"], f"{where}.request.start_utc")
    end_ms = to_epoch_ms(request["end_utc"], f"{where}.request.end_utc")
    coverage_end_ms = to_epoch_ms(facts["coverage_end_utc"], f"{where}.coverage_end_utc")
    # A stored tail reaching past an older requested end is covered, not negative.
    expected_shortfall = max(0, (end_ms - coverage_end_ms) // BASE_STEP_MS)
    if facts["tail_shortfall_bars"] != expected_shortfall:
        raise _journal_error(
            f"{where}.tail_shortfall_bars: {facts['tail_shortfall_bars']} is not the "
            f"{expected_shortfall} bar(s) between the coverage end {facts['coverage_end_utc']} and "
            f"the requested end {request['end_utc']}."
        )
    _validate_fetch_ranges(facts["fetch_ranges"], f"{where}.fetch_ranges", start_ms=start_ms, end_ms=end_ms)
    _validate_gap_ranges(
        facts["gap_ranges"],
        f"{where}.gap_ranges",
        first_ms=to_epoch_ms(facts["first_open_utc"], f"{where}.first_open_utc"),
        last_ms=to_epoch_ms(facts["last_open_utc"], f"{where}.last_open_utc"),
        range_count=facts["gap_range_count"],
        total_bars=facts["gap_bar_count"],
    )
    return facts


def _validate_staged_record(
    raw: Any,
    operation_id: str,
    where: str,
    *,
    base_files: Mapping[str, str],
    request: Mapping[str, Any],
) -> dict[str, Any]:
    record = _mapping(raw, where)
    instrument_id = pack_manifest.normalize_instrument_id(
        record.get("instrument_id"), f"{where}.instrument_id"
    )
    changed = record.get("changed")
    if not isinstance(changed, bool):
        raise _journal_error(f"{where}.changed: expected a boolean.")
    old = record.get("old_sha256")
    staged = record.get("staged_path")
    try:
        entry = pack_manifest.validate_instrument_entry(
            record.get("entry"), f"{where}.entry", managed=True
        )
    except PatternLabDataError as exc:
        raise _journal_error(str(exc)) from exc
    validated = {
        "instrument_id": instrument_id,
        "final_path": pack_manifest.validate_relative_file(record.get("final_path"), f"{where}.final_path"),
        "staged_path": None
        if staged is None
        else _relative_staged_path(staged, operation_id, f"{where}.staged_path"),
        "old_sha256": None if old is None else require_sha256(old, f"{where}.old_sha256"),
        "new_sha256": require_sha256(record.get("new_sha256"), f"{where}.new_sha256"),
        "changed": changed,
        "entry": entry,
        "facts": _validate_facts(
            record.get("facts"), entry, f"{where}.facts", request=request, changed=changed
        ),
    }
    extra = sorted(set(record) - set(validated))
    if extra:
        raise _journal_error(f"{where}: unexpected journal keys {extra}.")

    # Paths and digests are deterministic functions of the instrument identity,
    # so a record can never point recovery at another instrument's file.
    if entry["instrument_id"] != instrument_id:
        raise _journal_error(
            f"{where}.entry.instrument_id: {entry['instrument_id']!r} is not {instrument_id!r}."
        )
    expected_final = pack_manifest.instrument_relative_file(instrument_id)
    if validated["final_path"] != expected_final:
        raise _journal_error(
            f"{where}.final_path: must be {expected_final!r}, got {validated['final_path']!r}."
        )
    if entry["file"] != expected_final:
        raise _journal_error(f"{where}.entry.file: must be {expected_final!r}, got {entry['file']!r}.")
    if entry["sha256"] != validated["new_sha256"]:
        raise _journal_error(
            f"{where}.entry.sha256: {entry['sha256']} does not match the recorded new digest "
            f"{validated['new_sha256']}."
        )
    if changed:
        if validated["staged_path"] is None:
            raise _journal_error(
                f"{where}: a changed instrument must record its staged replacement path."
            )
        expected_staged = (
            f"{staging_dir_name(operation_id)}/{pack_manifest.instrument_file_name(instrument_id)}"
        )
        if validated["staged_path"] != expected_staged:
            raise _journal_error(
                f"{where}.staged_path: must be {expected_staged!r}, got {validated['staged_path']!r}."
            )
    elif validated["old_sha256"] != validated["new_sha256"]:
        raise _journal_error(f"{where}: an unchanged instrument cannot change its digest.")
    expected_old = base_files.get(instrument_id)
    if validated["old_sha256"] != expected_old:
        raise _journal_error(
            f"{where}.old_sha256: {validated['old_sha256']} is not the recorded base digest "
            f"{expected_old} for {instrument_id}."
        )
    return validated


def _validate_text_target(raw: Any, key: str, operation_id: str, where: str) -> dict[str, Any]:
    target = _mapping(raw, where)
    old = target.get("old_sha256")
    validated = {
        "final_name": TARGET_FINAL_NAMES[key],
        "staged_path": _relative_staged_path(target.get("staged_path"), operation_id, f"{where}.staged_path"),
        "old_sha256": None if old is None else require_sha256(old, f"{where}.old_sha256"),
        "new_sha256": require_sha256(target.get("new_sha256"), f"{where}.new_sha256"),
    }
    if target.get("final_name") not in (None, TARGET_FINAL_NAMES[key]):
        raise _journal_error(f"{where}.final_name: must be {TARGET_FINAL_NAMES[key]!r}.")
    extra = sorted(set(target) - set(validated))
    if extra:
        raise _journal_error(f"{where}: unexpected journal keys {extra}.")
    return validated


def _validate_request(raw: Any, where: str) -> dict[str, Any]:
    """Validate the resolved, canonical, nonempty request interval."""
    request = _mapping(raw, where)
    missing = sorted(set(REQUEST_KEYS) - set(request))
    extra = sorted(set(request) - set(REQUEST_KEYS))
    if missing or extra:
        raise _journal_error(
            f"{where}: the request object is closed; missing keys {missing}, unexpected keys {extra}."
        )
    try:
        start_ms = require_aligned_utc(request["start_utc"], f"{where}.start_utc")
        end_ms = require_aligned_utc(request["end_utc"], f"{where}.end_utc")
        requested_start = request["requested_start_utc"]
        requested_start_ms = (
            None if requested_start is None else require_aligned_utc(requested_start, f"{where}.requested_start_utc")
        )
        note = require_optional_text(request["note"], f"{where}.note")
    except PatternLabDataError as exc:
        raise _journal_error(str(exc)) from exc
    if start_ms >= end_ms:
        raise _journal_error(
            f"{where}: requires start_utc < end_utc, got {format_epoch_ms(start_ms)} >= "
            f"{format_epoch_ms(end_ms)}."
        )
    token = request["requested_end_token"]
    if token not in (None, exchange_data.LATEST_CLOSED):
        raise _journal_error(
            f"{where}.requested_end_token: expected null or {exchange_data.LATEST_CLOSED!r}, got {token!r}."
        )
    if requested_start_ms is not None and requested_start_ms < start_ms:
        raise _journal_error(
            f"{where}.requested_start_utc: the effective start {format_epoch_ms(start_ms)} must be at "
            f"or before the requested {format_epoch_ms(requested_start_ms)}."
        )
    return {
        "start_utc": format_epoch_ms(start_ms),
        "end_utc": format_epoch_ms(end_ms),
        "requested_end_token": token,
        "requested_start_utc": None
        if requested_start_ms is None
        else format_epoch_ms(requested_start_ms),
        "note": note,
    }


def _validate_closure(
    raw: Any, where: str, *, request: Mapping[str, Any], venues: Sequence[str]
) -> dict[str, Any]:
    """Validate the frozen closure evidence and its agreement with the request.

    The recorded clock samples must cover exactly the frozen roster's distinct
    venues: a missing venue is never accepted merely because the remaining
    samples happen to give the same cutoff.
    """
    closure = _mapping(raw, where)
    missing = sorted(set(CLOSURE_KEYS) - set(closure))
    extra = sorted(set(closure) - set(CLOSURE_KEYS))
    if missing or extra:
        raise _journal_error(
            f"{where}: the closure object is closed; missing keys {missing}, unexpected keys {extra}."
        )
    samples = _mapping(closure["server_times_ms"], f"{where}.server_times_ms")
    if not samples:
        raise _journal_error(f"{where}.server_times_ms: at least one venue server time is required.")
    try:
        times = {
            pack_manifest.normalize_id_part(venue, f"{where}.server_times_ms key"): require_int(
                value, f"{where}.server_times_ms[{venue}]", minimum=1
            )
            for venue, value in samples.items()
        }
        lag_ms = require_int(closure["publication_lag_ms"], f"{where}.publication_lag_ms", minimum=0)
        cutoff_ms = require_aligned(
            require_int(closure["safe_cutoff_ms"], f"{where}.safe_cutoff_ms"),
            BASE_STEP_MS,
            f"{where}.safe_cutoff_ms",
        )
        resolved_ms = require_aligned(
            require_int(closure["resolved_end_ms"], f"{where}.resolved_end_ms"),
            BASE_STEP_MS,
            f"{where}.resolved_end_ms",
        )
        observed = format_utc(closure["observed_utc"], f"{where}.observed_utc")
    except PatternLabDataError as exc:
        raise _journal_error(str(exc)) from exc
    expected_venues = sorted(set(venues))
    if sorted(times) != expected_venues:
        raise _journal_error(
            f"{where}.server_times_ms: the frozen clock samples must cover exactly the roster venues "
            f"{expected_venues}, got {sorted(times)}."
        )
    if lag_ms != exchange_data.CLOSURE_LAG_MS:
        raise _journal_error(
            f"{where}.publication_lag_ms: {lag_ms} is not the fixed {exchange_data.CLOSURE_LAG_MS}ms "
            "publication allowance this build applies."
        )
    expected_cutoff = ((min(times.values()) - lag_ms) // BASE_STEP_MS) * BASE_STEP_MS
    if cutoff_ms != expected_cutoff:
        raise _journal_error(
            f"{where}.safe_cutoff_ms: {cutoff_ms} is not the cutoff {expected_cutoff} derived from the "
            "recorded server samples and publication allowance."
        )
    if resolved_ms > cutoff_ms:
        raise _journal_error(
            f"{where}.resolved_end_ms: {format_epoch_ms(resolved_ms)} is beyond the frozen safe closed "
            f"cutoff {format_epoch_ms(cutoff_ms)}."
        )
    if resolved_ms != to_epoch_ms(request["end_utc"], f"{where}.resolved_end_ms"):
        raise _journal_error(
            f"{where}.resolved_end_ms: {format_epoch_ms(resolved_ms)} does not agree with the effective "
            f"request end {request['end_utc']}."
        )
    if closure["requested_end_token"] != request["requested_end_token"]:
        raise _journal_error(
            f"{where}.requested_end_token: {closure['requested_end_token']!r} does not agree with the "
            f"request token {request['requested_end_token']!r}."
        )
    if request["requested_end_token"] == exchange_data.LATEST_CLOSED and resolved_ms != cutoff_ms:
        raise _journal_error(
            f"{where}.resolved_end_ms: {exchange_data.LATEST_CLOSED!r} resolves to the frozen safe "
            f"cutoff {format_epoch_ms(cutoff_ms)}, not {format_epoch_ms(resolved_ms)}."
        )
    return {
        "server_times_ms": times,
        "observed_utc": observed,
        "publication_lag_ms": lag_ms,
        "safe_cutoff_ms": cutoff_ms,
        "resolved_end_ms": resolved_ms,
        "requested_end_token": request["requested_end_token"],
    }


def _validate_probe(raw: Any, where: str, *, start_ms: int) -> dict[str, Any]:
    """Validate one closed start-boundary probe record.

    A checked probe is the only evidence that a requested managed start is
    actually retrievable, so it must record that the slot was available.  An
    unchecked probe records no observation at all and represents a start that is
    already covered by the stored pack.
    """
    probe = _mapping(raw, where)
    missing = sorted(set(PROBE_KEYS) - set(probe))
    extra = sorted(set(probe) - set(PROBE_KEYS))
    if missing or extra:
        raise _journal_error(
            f"{where}: the probe record is closed; missing keys {missing}, unexpected keys {extra}."
        )
    try:
        checked = require_bool(probe["checked"], f"{where}.checked")
        slot_ms = require_aligned_utc(probe["slot_utc"], f"{where}.slot_utc")
        reason = require_text(probe["reason"], f"{where}.reason")
    except PatternLabDataError as exc:
        raise _journal_error(str(exc)) from exc
    if slot_ms != start_ms:
        raise _journal_error(
            f"{where}.slot_utc: {format_epoch_ms(slot_ms)} is not the effective requested start "
            f"{format_epoch_ms(start_ms)}."
        )
    available = probe["available"]
    if checked and available is not True:
        raise _journal_error(
            f"{where}.available: a checked probe retained as valid evidence must record an available "
            f"first slot, got {available!r}."
        )
    if not checked and available is not None:
        raise _journal_error(
            f"{where}.available: an unchecked probe made no observation, so available must be null, "
            f"got {available!r}."
        )
    return {
        "checked": checked,
        "slot_utc": format_epoch_ms(slot_ms),
        "available": available,
        "reason": reason,
    }


def _validate_preflight(
    raw: Any,
    where: str,
    *,
    roster: Sequence[Mapping[str, Any]],
    request: Mapping[str, Any],
    kind: str,
    phase: str,
    completed: Sequence[str],
) -> dict[str, Any]:
    """Validate the recorded preflight evidence before any recovery work.

    Restored evidence must be usable as it stands: rules pass the venue-aware
    managed-rule validator, the probe relates to this operation's effective start,
    and the recorded listing agrees with both the rules and that start.  The
    producer checkpoints the whole roster's preflight at once, so once any is
    recorded all of it is required; the legitimate initial state before preflight
    is an empty mapping with nothing completed.  Nothing here fabricates missing
    evidence or quietly re-runs an invalid completed preflight.
    """
    evidence = _mapping(raw, where)
    venues = {entry["instrument_id"]: entry["venue"] for entry in roster}
    if not evidence:
        if completed or phase == "applying":
            raise _journal_error(
                f"{where}: completed work requires the recorded all-roster preflight evidence, but "
                f"none is recorded ({sorted(completed)} completed, phase {phase!r})."
            )
        return {}
    absent = sorted(set(venues) - set(evidence))
    if absent:
        raise _journal_error(
            f"{where}: preflight is checkpointed for the whole roster at once, so a recorded "
            f"preflight must cover every member; {absent} are missing."
        )
    start_ms = to_epoch_ms(request["start_utc"], f"{where}.request.start_utc")
    validated: dict[str, Any] = {}
    for key, value in evidence.items():
        position = f"{where}[{key}]"
        if key not in venues:
            raise _journal_error(f"{position}: {key!r} is not a member of the recorded roster.")
        item = _mapping(value, position)
        missing = sorted(set(PREFLIGHT_KEYS) - set(item))
        extra = sorted(set(item) - set(PREFLIGHT_KEYS))
        if missing or extra:
            raise _journal_error(
                f"{position}: the preflight record is closed; missing keys {missing}, unexpected "
                f"keys {extra}."
            )
        listed = item["listed_at_utc"]
        try:
            rules = pack_manifest.validate_instrument_rules(
                item["rules"], f"{position}.rules", venue=venues[key]
            )
            record = {
                "rules": rules,
                "probe": _validate_probe(item["probe"], f"{position}.probe", start_ms=start_ms),
                "listing_known": require_bool(item["listing_known"], f"{position}.listing_known"),
                "listed_at_utc": None if listed is None else format_utc(listed, f"{position}.listed_at_utc"),
            }
        except PatternLabDataError as exc:
            raise _journal_error(str(exc)) from exc
        if record["listing_known"] != (record["listed_at_utc"] is not None):
            raise _journal_error(
                f"{position}.listing_known: does not agree with the recorded listed_at_utc {listed!r}."
            )
        if record["listed_at_utc"] != rules["listed_at_utc"]:
            raise _journal_error(
                f"{position}.listed_at_utc: {record['listed_at_utc']!r} does not agree with the "
                f"recorded rules listing {rules['listed_at_utc']!r}."
            )
        if record["listed_at_utc"] is not None:
            listed_ms = to_epoch_ms(record["listed_at_utc"], f"{position}.listed_at_utc")
            if listed_ms > start_ms:
                raise _journal_error(
                    f"{position}.listed_at_utc: the venue lists this contract at "
                    f"{record['listed_at_utc']}, after the effective requested start "
                    f"{format_epoch_ms(start_ms)}; that is not valid retained preflight evidence."
                )
        if kind == "collect" and not record["probe"]["checked"]:
            raise _journal_error(
                f"{position}.probe.checked: an initial collect proves its requested start with a "
                "start-boundary probe; no stored coverage can already cover it."
            )
        validated[key] = record
    return validated


def validate_journal(raw: Any, *, where: str = "journal") -> dict[str, Any]:
    """Validate the versioned operation journal before any journal-driven write.

    Relationships derivable from the saved state are checked here, not only
    container types: the canonical request and closure agree and the clock
    samples cover the frozen roster's venues, options are the valid frozen ones,
    restored preflight rules/probe/listing evidence is usable as it stands, each
    record's derived facts follow from its request and staged entry, the staged
    set is a roster subset with deterministic paths and digests, and the target
    revision follows from the recorded base.
    """
    journal = _mapping(raw, where)
    version = require_int(journal.get("journal_version"), f"{where}.journal_version")
    if version != JOURNAL_VERSION:
        raise _journal_error(
            f"{where}.journal_version: unsupported version {version}; this build writes {JOURNAL_VERSION}."
        )
    operation_id = require_text(journal.get("operation_id"), f"{where}.operation_id")
    if not _OPERATION_ID_RE.fullmatch(operation_id):
        raise _journal_error(f"{where}.operation_id: unsafe operation identifier {operation_id!r}.")
    kind = journal.get("kind")
    if kind not in OPERATION_KINDS:
        raise _journal_error(f"{where}.kind: expected one of {list(OPERATION_KINDS)}, got {kind!r}.")
    phase = journal.get("phase")
    if phase not in PHASES:
        raise _journal_error(f"{where}.phase: expected one of {list(PHASES)}, got {phase!r}.")
    if journal.get("staging_dir") != staging_dir_name(operation_id):
        raise _journal_error(
            f"{where}.staging_dir: must be {staging_dir_name(operation_id)!r}, "
            f"got {journal.get('staging_dir')!r}."
        )

    base = _mapping(journal.get("base"), f"{where}.base")
    base_revision = base.get("revision")
    base_files = _mapping(base.get("files"), f"{where}.base.files")
    validated_base = {
        "revision": None if base_revision is None else require_int(base_revision, f"{where}.base.revision", minimum=1),
        "manifest_sha256": _optional_sha(base.get("manifest_sha256"), f"{where}.base.manifest_sha256"),
        "readme_sha256": _optional_sha(base.get("readme_sha256"), f"{where}.base.readme_sha256"),
        "updates_sha256": _optional_sha(base.get("updates_sha256"), f"{where}.base.updates_sha256"),
        "files": {
            pack_manifest.normalize_instrument_id(key, f"{where}.base.files key"): require_sha256(
                value, f"{where}.base.files[{key}]"
            )
            for key, value in base_files.items()
        },
    }
    target_revision = require_int(
        journal.get("target_revision"), f"{where}.target_revision", minimum=1
    )
    if kind == "collect":
        if validated_base["revision"] is not None:
            raise _journal_error(f"{where}.base.revision: an initial collect has no base revision.")
        stale = sorted(
            key
            for key in ("manifest_sha256", "readme_sha256", "updates_sha256")
            if validated_base[key] is not None
        )
        if stale or validated_base["files"]:
            raise _journal_error(
                f"{where}.base: an initial collect requires null base facts, but it records {stale} "
                f"and {sorted(validated_base['files'])}."
            )
        if target_revision != 1:
            raise _journal_error(
                f"{where}.target_revision: an initial collect publishes revision 1, got {target_revision}."
            )
    else:
        if validated_base["revision"] is None:
            raise _journal_error(f"{where}.base.revision: an update must record the base revision.")
        if target_revision != validated_base["revision"] + 1:
            raise _journal_error(
                f"{where}.target_revision: an update of revision {validated_base['revision']} publishes "
                f"{validated_base['revision'] + 1}, got {target_revision}."
            )

    roster = pack_manifest.validate_roster_entries(journal.get("roster"), f"{where}.roster")
    roster_ids = [entry["instrument_id"] for entry in roster]
    request = _validate_request(journal.get("request"), f"{where}.request")
    # The frozen request and closure are settled before any record is related to
    # them, so a disagreeing request is reported as such rather than as a
    # downstream per-instrument contradiction.
    closure = _validate_closure(
        journal.get("closure"),
        f"{where}.closure",
        request=request,
        venues=[entry["venue"] for entry in roster],
    )
    staged_raw = _mapping(journal.get("staged"), f"{where}.staged")
    staged: dict[str, Any] = {}
    for key, value in staged_raw.items():
        if key not in roster_ids:
            raise _journal_error(
                f"{where}.staged[{key}]: {key!r} is not a member of the recorded roster."
            )
        record = _validate_staged_record(
            value,
            operation_id,
            f"{where}.staged[{key}]",
            base_files=validated_base["files"],
            request=request,
        )
        if record["instrument_id"] != key:
            raise _journal_error(f"{where}.staged[{key}]: record declares {record['instrument_id']!r}.")
        staged[key] = record

    targets_raw = journal.get("targets")
    targets: dict[str, Any] = {}
    if targets_raw is not None:
        mapping = _mapping(targets_raw, f"{where}.targets")
        missing = sorted(set(TEXT_TARGETS) - set(mapping))
        extra = sorted(set(mapping) - set(TEXT_TARGETS))
        if missing or extra:
            raise _journal_error(
                f"{where}.targets: expected exactly {list(TEXT_TARGETS)}; missing {missing}, unexpected {extra}."
            )
        targets = {
            key: _validate_text_target(mapping[key], key, operation_id, f"{where}.targets.{key}")
            for key in TEXT_TARGETS
        }
    if phase == "applying":
        if not targets:
            raise _journal_error(f"{where}.targets: the applying phase requires frozen target metadata.")
        incomplete = sorted(set(roster_ids) - set(staged))
        if incomplete:
            raise _journal_error(
                f"{where}.staged: the applying phase requires every roster instrument, but "
                f"{incomplete} were never completed."
            )

    try:
        options = exchange_data.validate_http_options(journal.get("options"), where=f"{where}.options")
    except PatternLabDataError as exc:
        raise _journal_error(str(exc)) from exc
    validated = {
        "journal_version": version,
        "operation_id": operation_id,
        "kind": kind,
        "root": require_text(journal.get("root"), f"{where}.root"),
        "staging_dir": staging_dir_name(operation_id),
        "operation_started_utc": format_utc(
            journal.get("operation_started_utc"), f"{where}.operation_started_utc"
        ),
        "request": request,
        "options": options,
        "universe": _mapping(journal.get("universe"), f"{where}.universe"),
        "roster": roster,
        "roster_sha256": require_sha256(journal.get("roster_sha256"), f"{where}.roster_sha256"),
        "base": validated_base,
        "target_revision": target_revision,
        "phase": phase,
        "closure": closure,
        "preflight": _validate_preflight(
            journal.get("preflight"),
            f"{where}.preflight",
            roster=roster,
            request=request,
            kind=kind,
            phase=phase,
            completed=sorted(staged),
        ),
        "staged": staged,
        "targets": targets or None,
    }
    if pack_manifest.roster_sha256(roster) != validated["roster_sha256"]:
        raise _journal_error(f"{where}.roster_sha256: does not match the recorded roster.")
    extra = sorted(set(journal) - set(validated))
    if extra:
        raise _journal_error(f"{where}: unexpected journal keys {extra}.")
    return validated


def _optional_sha(value: Any, where: str) -> str | None:
    return None if value is None else require_sha256(value, where)


# --------------------------------------------------------------------------
# journal IO and pending detection
# --------------------------------------------------------------------------

def write_journal(data_root: Path, journal: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and durably publish the operation journal through its own temp."""
    validated = validate_journal(journal)
    pack_manifest.write_text_atomic(
        marker_path(data_root), pack_manifest.dumps_json(validated) + "\n"
    )
    return validated


def _read_journal_file(path: Path) -> dict[str, Any]:
    return validate_journal(pack_manifest.read_json_file(path, source=str(path)))


def pending_state(data_root: Path) -> dict[str, Any] | None:
    """Describe any pending operation without adopting or deleting it.

    Returns ``None`` when neither the canonical marker nor its initial temporary
    is present.  A malformed artifact is reported with ``valid=False`` and a
    problem description; the caller decides whether that is an error, because a
    malformed marker must be inspected manually rather than silently removed.
    """
    for source, path in (("marker", marker_path(data_root)), ("temporary", marker_temp_path(data_root))):
        if not path.is_file():
            continue
        try:
            journal = _read_journal_file(path)
        except PatternLabDataError as exc:
            return {"source": source, "path": str(path), "valid": False, "problem": str(exc), "journal": None}
        return {"source": source, "path": str(path), "valid": True, "problem": None, "journal": journal}
    return None


def pending_summary(state: Mapping[str, Any]) -> dict[str, Any]:
    """Return the JSON-ready status of a pending operation for inspect output."""
    if not state["valid"]:
        return {
            "source": state["source"],
            "path": state["path"],
            "valid": False,
            "problem": state["problem"],
        }
    journal = state["journal"]
    return {
        "source": state["source"],
        "path": state["path"],
        "valid": True,
        "problem": None,
        "operation_id": journal["operation_id"],
        "kind": journal["kind"],
        "phase": journal["phase"],
        "operation_started_utc": journal["operation_started_utc"],
        "request": dict(journal["request"]),
        "base_revision": journal["base"]["revision"],
        "target_revision": journal["target_revision"],
        "roster_size": len(journal["roster"]),
        "completed_instruments": sorted(journal["staged"]),
        "completed_instrument_count": len(journal["staged"]),
        "targets_frozen": journal["targets"] is not None,
    }


def pending_problem(state: Mapping[str, Any]) -> str:
    """Return the actionable one-line description used by readers and inspect."""
    if not state["valid"]:
        return (
            f"{state['path']}: a malformed pending-operation journal is present ({state['problem']}). "
            "Inspect it manually; Pattern Lab never removes or adopts it automatically."
        )
    journal = state["journal"]
    return (
        f"{state['path']}: operation {journal['operation_id']} ({journal['kind']}) is pending in the "
        f"{journal['phase']} phase; run 'recover' to finish it, or 'abort-update' while it is still "
        "staging. Research reads refuse a pending generation."
    )


# --------------------------------------------------------------------------
# staging directory and artifact helpers
# --------------------------------------------------------------------------

def ensure_staging_dir(data_root: Path, operation_id: str) -> Path:
    path = staging_dir(data_root, operation_id)
    if path.is_symlink():
        raise PatternLabDataError(
            f"{path}: the staging directory must not be a symlink.", error_code="unsafe_path"
        )
    path.mkdir(exist_ok=True)
    pack_manifest.fsync_directory(Path(data_root))
    return path


def resolve_staged(data_root: Path, relative: str, operation_id: str, where: str) -> Path:
    """Resolve one journalled staged path, rejecting escapes and symlinks."""
    _relative_staged_path(relative, operation_id, where)
    root = Path(data_root).resolve()
    target = (root / relative).resolve()
    if not target.is_relative_to(root / staging_dir_name(operation_id)):
        raise _journal_error(f"{where}: {relative!r} resolves outside this operation's staging directory.")
    if Path(root / relative).is_symlink():
        raise PatternLabDataError(f"{where}: {relative!r} is a symlink.", error_code="unsafe_path")
    return Path(root / relative)


def write_staged_text(data_root: Path, operation_id: str, name: str, text: str) -> dict[str, Any]:
    """Write one frozen metadata artifact into the staging directory."""
    directory = ensure_staging_dir(data_root, operation_id)
    path = directory / name
    pack_manifest.write_text_atomic(path, text)
    return {
        "staged_path": f"{staging_dir_name(operation_id)}/{name}",
        "new_sha256": file_sha256(path),
    }


def digest_or_none(path: Path) -> str | None:
    return file_sha256(path) if Path(path).is_file() else None


# --------------------------------------------------------------------------
# applying: forward-only publication and recovery
# --------------------------------------------------------------------------

def _unrecoverable(message: str) -> PatternLabDataError:
    return PatternLabDataError(
        message
        + " Restore the exact missing staged artifact from an available copy, or collect a separate "
        "new root. Preserve this root and its journal; never clear the marker to make a partially "
        "replaced pack readable.",
        error_code="unrecoverable_staged_artifact",
    )


@dataclass(frozen=True)
class _Replacement:
    """One validated pending move from a staged artifact to its destination."""

    destination: Path
    staged: Path
    relative: str | None
    label: str | None


def _plan_replacement(
    data_root: Path,
    *,
    destination: Path,
    staged_relative: str | None,
    old_sha256: str | None,
    new_sha256: str,
    operation_id: str,
    where: str,
) -> Path | None:
    """Validate one planned replacement and return the staged source, or None.

    The destination must match either the recorded old digest (the step has not
    run) or the recorded new digest (it completed before the crash).  A third
    value is unexpected modification and blocks recovery.
    """
    current = digest_or_none(destination)
    if current == new_sha256:
        return None
    if current != old_sha256:
        raise PatternLabDataError(
            f"{where}: {destination} has SHA-256 {current} but the journal recorded "
            f"{old_sha256} before the replacement and {new_sha256} after it. The file was modified "
            "outside this operation; recovery stops rather than guessing.",
            error_code="unexpected_target_state",
        )
    if staged_relative is None:
        raise _unrecoverable(f"{where}: no staged replacement was recorded for {destination}.")
    staged = resolve_staged(data_root, staged_relative, operation_id, f"{where}.staged_path")
    if not staged.is_file():
        raise _unrecoverable(f"{where}: the staged artifact {staged} is missing.")
    actual = file_sha256(staged)
    if actual != new_sha256:
        raise PatternLabDataError(
            f"{where}: the staged artifact {staged} has SHA-256 {actual}, not the recorded "
            f"{new_sha256}.",
            error_code="unexpected_target_state",
        )
    return staged


def frozen_target_text(data_root: Path, journal: Mapping[str, Any], key: str) -> str:
    """Return one frozen metadata artifact's exact bytes, staged or published.

    A staged artifact consumed by a completed rename is not corruption: the
    already-published destination carrying the recorded new digest is the same
    frozen bytes, so recovery reads it from there instead.
    """
    target = journal["targets"][key]
    root = Path(data_root)
    where = f"targets.{key}"
    staged = resolve_staged(root, target["staged_path"], journal["operation_id"], f"{where}.staged_path")
    for candidate in (staged, root / target["final_name"]):
        if candidate.is_file() and file_sha256(candidate) == target["new_sha256"]:
            return candidate.read_text(encoding="utf-8")
    raise _unrecoverable(
        f"{where}: neither the staged artifact {staged} nor the published {target['final_name']} "
        f"carries the frozen digest {target['new_sha256']}."
    )


def _validate_target_manifest(data_root: Path, journal: Mapping[str, Any]) -> None:
    """Require the frozen target manifest to agree with the journal's plan."""
    text = frozen_target_text(data_root, journal, "manifest")
    try:
        manifest = pack_manifest.validate_manifest(
            pack_manifest.loads_strict(text, source="targets.manifest")
        )
    except PatternLabDataError as exc:
        raise _journal_error(f"targets.manifest: the frozen manifest is invalid ({exc}).") from exc
    collector = manifest.get("collector")
    if collector is None:
        raise _journal_error("targets.manifest: a collector operation must publish a managed manifest.")
    mismatches = []
    if manifest["revision"] != journal["target_revision"]:
        mismatches.append(f"revision {manifest['revision']} versus target {journal['target_revision']}")
    if collector["operation_id"] != journal["operation_id"]:
        mismatches.append(
            f"operation {collector['operation_id']!r} versus {journal['operation_id']!r}"
        )
    if collector["roster_sha256"] != journal["roster_sha256"] or collector["roster"] != journal["roster"]:
        mismatches.append("a different roster")
    expected_request = {
        "start_utc": journal["request"]["start_utc"],
        "end_utc": journal["request"]["end_utc"],
    }
    if collector["last_request"] != expected_request:
        mismatches.append(f"last_request {collector['last_request']} versus {expected_request}")
    published = {entry["instrument_id"]: entry for entry in manifest["instruments"]}
    if sorted(published) != sorted(journal["staged"]):
        mismatches.append(f"instruments {sorted(published)} versus staged {sorted(journal['staged'])}")
    else:
        for instrument_id, record in sorted(journal["staged"].items()):
            if published[instrument_id] != record["entry"]:
                mismatches.append(f"a different entry for {instrument_id}")
    if mismatches:
        raise _journal_error(
            "targets.manifest: the frozen target manifest disagrees with the journal ("
            + "; ".join(mismatches)
            + "). Nothing was replaced."
        )


def build_apply_plan(data_root: Path, journal: Mapping[str, Any]) -> list[_Replacement]:
    """Validate the complete plan and return its pending moves, in publish order.

    Every destination, staged artifact and the frozen target manifest are checked
    before the first live replacement, so a valid-looking subset is never
    published ahead of discovering that another planned target is inconsistent.
    """
    root = Path(data_root)
    if journal["phase"] != "applying":
        raise _journal_error("apply: the journal is not in the applying phase.")
    _validate_target_manifest(root, journal)

    moves: list[_Replacement] = []
    for instrument_id in sorted(journal["staged"]):
        record = journal["staged"][instrument_id]
        where = f"staged[{instrument_id}]"
        destination = pack_manifest.resolve_pack_path(root, record["final_path"], f"{where}.final_path")
        if not record["changed"]:
            actual = digest_or_none(destination)
            if actual != record["new_sha256"]:
                raise PatternLabDataError(
                    f"{where}: the unchanged file {destination} has SHA-256 {actual}, not the "
                    f"recorded {record['new_sha256']}.",
                    error_code="unexpected_target_state",
                )
            continue
        staged = _plan_replacement(
            root,
            destination=destination,
            staged_relative=record["staged_path"],
            old_sha256=record["old_sha256"],
            new_sha256=record["new_sha256"],
            operation_id=journal["operation_id"],
            where=where,
        )
        if staged is not None:
            moves.append(_Replacement(destination, staged, record["final_path"], instrument_id))
    # History and README first, the manifest last: a ready manifest is the only
    # signal that the whole generation is published.
    for key in TEXT_TARGETS:
        target = journal["targets"][key]
        staged = _plan_replacement(
            root,
            destination=root / target["final_name"],
            staged_relative=target["staged_path"],
            old_sha256=target["old_sha256"],
            new_sha256=target["new_sha256"],
            operation_id=journal["operation_id"],
            where=f"targets.{key}",
        )
        if staged is not None:
            moves.append(_Replacement(root / target["final_name"], staged, None, None))
    return moves


def _publish_replacement(move: _Replacement) -> None:
    """Perform one already-validated move and flush its containing directory."""
    os.replace(move.staged, move.destination)
    pack_manifest.fsync_directory(move.destination.parent)


def apply_operation(
    data_root: Path, journal: Mapping[str, Any], *, progress=None
) -> dict[str, Any]:
    """Publish the frozen targets, then clean up, then remove the marker last.

    This is the only forward path once ``applying`` has been recorded, and it is
    idempotent: every step recognizes its own completed result by digest, so a
    resumed operation publishes exactly one revision and one history event.
    """
    root = Path(data_root)
    replaced: list[str] = []
    for move in build_apply_plan(root, journal):
        _publish_replacement(move)
        if move.relative is not None:
            replaced.append(move.relative)
        if progress is not None and move.label is not None:
            progress(f"applied {move.label}")

    _verify_targets(root, journal)
    cleanup_operation(root, journal)
    remove_marker(root)
    return {"replaced_files": replaced}


def _verify_targets(data_root: Path, journal: Mapping[str, Any]) -> None:
    """Verify every published artifact against the journal before cleanup."""
    root = Path(data_root)
    for instrument_id in sorted(journal["staged"]):
        record = journal["staged"][instrument_id]
        destination = pack_manifest.resolve_pack_path(
            root, record["final_path"], f"staged[{instrument_id}].final_path"
        )
        actual = digest_or_none(destination)
        if actual != record["new_sha256"]:
            raise PatternLabDataError(
                f"staged[{instrument_id}]: published {destination} has SHA-256 {actual}, not the "
                f"target {record['new_sha256']}.",
                error_code="unexpected_target_state",
            )
    for key in TEXT_TARGETS:
        target = journal["targets"][key]
        path = root / target["final_name"]
        actual = digest_or_none(path)
        if actual != target["new_sha256"]:
            raise PatternLabDataError(
                f"targets.{key}: published {path} has SHA-256 {actual}, not the target "
                f"{target['new_sha256']}.",
                error_code="unexpected_target_state",
            )


def owned_staging_names(journal: Mapping[str, Any]) -> set[str]:
    """Return every completed file name this operation may create while staging.

    The set is derived from the validated frozen roster and the fixed metadata
    target plan, so a file that was written durably but interrupted before its
    journal checkpoint is still recognized as this operation's own work.  A name
    identifies incomplete owned work for removal and re-download; only a
    validated checkpoint and digest ever permit a staged file to be reused.
    """
    names = {
        pack_manifest.instrument_file_name(entry["instrument_id"]) for entry in journal["roster"]
    }
    return names | set(TARGET_FINAL_NAMES.values())


def is_owned_staging_name(name: str, owned: set[str]) -> bool:
    """Return whether one staging entry matches this operation's exact conventions."""
    if name in owned:
        return True
    temporary = _STAGED_TEMP_RE.fullmatch(name)  # Parquet: .<name>.tmp-<uuid>
    if temporary is not None:
        return temporary.group("base") in owned
    if name.startswith(".") and name.endswith(".tmp"):  # text: .<name>.tmp
        return name[1:-len(".tmp")] in owned
    return False


def cleanup_operation(data_root: Path, journal: Mapping[str, Any]) -> list[str]:
    """Remove only this operation's own artifacts and its staging directory.

    Cleanup is idempotent: an already-consumed artifact is simply absent.  Any
    entry that is not one of this operation's exact owned names — including a
    symlink or a directory — is preserved and reported, and the staging
    directory is then retained with its journal for inspection.
    """
    root = Path(data_root)
    directory = staging_dir(root, journal["operation_id"])
    if directory.is_symlink():
        raise PatternLabDataError(
            f"{directory}: the staging directory must not be a symlink.", error_code="unsafe_path"
        )
    removed: list[str] = []
    if not directory.is_dir():
        return removed
    owned = owned_staging_names(journal)
    unexpected: list[str] = []
    for child in sorted(directory.iterdir()):
        # A symlink is never followed and a directory is never recursed into.
        if child.is_symlink() or not child.is_file() or not is_owned_staging_name(child.name, owned):
            unexpected.append(child.name)
            continue
        child.unlink()
        removed.append(child.name)
    if unexpected:
        raise PatternLabDataError(
            f"{directory}: unexpected artifacts {unexpected} are present in this operation's staging "
            "directory. They are not part of the recorded plan, so they are preserved and reported "
            "rather than deleted; the operation journal is retained. Inspect them, move them "
            "elsewhere, and run the command again.",
            error_code="unexpected_staging_artifact",
        )
    directory.rmdir()
    pack_manifest.fsync_directory(root)
    return removed


def remove_marker(data_root: Path) -> None:
    """Remove the operation marker last, after every target is published."""
    root = Path(data_root)
    marker_path(root).unlink(missing_ok=True)
    marker_temp_path(root).unlink(missing_ok=True)
    pack_manifest.fsync_directory(root)


def abort_operation(data_root: Path, journal: Mapping[str, Any]) -> dict[str, Any]:
    """Abort a staging operation, leaving the base pack byte-for-byte unchanged."""
    root = Path(data_root)
    if journal["phase"] != "staging":
        raise PatternLabDataError(
            f"operation {journal['operation_id']} has already entered the applying phase and cannot be "
            "aborted; finish it forward with 'recover'.",
            error_code="abort_after_applying",
        )
    verify_base_intact(root, journal)
    removed = cleanup_operation(root, journal)
    remove_marker(root)
    return {"removed_artifacts": removed}


def verify_base_intact(data_root: Path, journal: Mapping[str, Any]) -> None:
    """Confirm the live files and metadata still match the recorded base."""
    root = Path(data_root)
    base = journal["base"]
    for name, digest in (
        (MANIFEST_NAME, base["manifest_sha256"]),
        (README_NAME, base["readme_sha256"]),
        (UPDATES_NAME, base["updates_sha256"]),
    ):
        actual = digest_or_none(root / name)
        if actual != digest:
            raise PatternLabDataError(
                f"{root / name}: has SHA-256 {actual} but the operation recorded {digest} as its base; "
                "the live pack changed outside this operation.",
                error_code="unexpected_target_state",
            )
    for instrument_id, digest in sorted(base["files"].items()):
        path = root / pack_manifest.instrument_relative_file(instrument_id)
        actual = digest_or_none(path)
        if actual != digest:
            raise PatternLabDataError(
                f"{path}: has SHA-256 {actual} but the operation recorded {digest} as its base; the "
                "live pack changed outside this operation.",
                error_code="unexpected_target_state",
            )


def remove_empty_created_directories(data_root: Path, names: Iterable[str]) -> list[str]:
    """Remove task-created subdirectories of an aborted initial collect if still empty.

    The data root itself and its persistent ``.pack-lock`` are always retained, so
    a completed initial abort leaves a reusable lock-only root.
    """
    root = Path(data_root)
    removed: list[str] = []
    for name in names:
        path = root / name
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
            removed.append(name)
    if removed:
        pack_manifest.fsync_directory(root)
    return removed


def unexpected_root_artifacts(data_root: Path, allowed: Sequence[str]) -> list[str]:
    """Return the entries of a destination that block a new initial collect."""
    root = Path(data_root)
    permitted = set(allowed)
    return sorted(item.name for item in root.iterdir() if item.name not in permitted)
