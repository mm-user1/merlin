"""Identifier, manifest, README and update-history contracts for data packs.

This module owns pack metadata only: it never reads or writes Parquet and never
imports PyArrow, so metadata can be validated in environments without the
optional wheel.  It is the single manifest serializer and README renderer shared
by the T01 importer and by later collector work.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, Mapping, Sequence

from . import PatternLabDataError

SCHEMA_VERSION = 1
BASE_TIMEFRAME_MINUTES = 5
BASE_STEP_MS = BASE_TIMEFRAME_MINUTES * 60_000
VOLUME_UNIT = "quote_turnover"

MANIFEST_NAME = "manifest.json"
README_NAME = "README.md"
UPDATES_NAME = "updates.jsonl"
OHLCV_DIR = "ohlcv"
UPDATE_MARKER_NAME = ".update-in-progress.json"
# write_text_atomic's sibling temporary for the marker, owned by the journal.
UPDATE_MARKER_TEMP_NAME = f".{UPDATE_MARKER_NAME}.tmp"

COLLECTOR_SCHEMA_VERSION = 1
INSTRUMENT_RULES_SCHEMA_VERSION = 1
ROSTER_KEYS = ("contract", "instrument_id", "quote_currency", "roles", "symbol", "venue")
CONTRACT_TYPES = ("linear_perpetual",)
TRADING_STATUSES = ("live", "Trading")

PACK_STATES = ("ready", "incomplete")
KNOWN_ROLES = ("factor", "research_only", "trading")
LEGAL_ROLE_SETS = (
    frozenset({"trading"}),
    frozenset({"research_only"}),
    frozenset({"factor"}),
    frozenset({"trading", "factor"}),
)
MEMBERSHIP_STATES = ("unknown", "verified")

_ID_PART_RE = re.compile(r"^[A-Z0-9][A-Z0-9.-]*$")
_FILE_NAME_RE = re.compile(r"^[A-Z0-9][A-Za-z0-9._-]*\.parquet$")
_CURRENCY_RE = re.compile(r"^[A-Z][A-Z0-9]{1,9}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


# --------------------------------------------------------------------------
# identifiers and paths
# --------------------------------------------------------------------------

def normalize_id_part(value: Any, field: str) -> str:
    """Normalize one venue or contract component to a safe uppercase ASCII ID."""
    if isinstance(value, bool) or not isinstance(value, str):
        raise PatternLabDataError(f"{field}: expected a string, got {type(value).__name__}.")
    if not value.isascii():
        raise PatternLabDataError(f"{field}: must be ASCII, got {value!r}.")
    if "_" in value:
        raise PatternLabDataError(f"{field}: underscore is reserved for the instrument-ID delimiter, got {value!r}.")
    normalized = value.upper()
    if not _ID_PART_RE.fullmatch(normalized):
        raise PatternLabDataError(
            f"{field}: must match ^[A-Z0-9][A-Z0-9.-]*$ after uppercasing, got {value!r}."
        )
    return normalized


def build_instrument_id(venue: Any, contract: Any, *, where: str = "instrument") -> str:
    """Return the canonical ``<VENUE>_<CONTRACT>`` identifier."""
    return f"{normalize_id_part(venue, f'{where}.venue')}_{normalize_id_part(contract, f'{where}.contract')}"


def normalize_instrument_id(value: Any, field: str = "instrument_id") -> str:
    """Validate a complete instrument identifier and return its normalized form."""
    if isinstance(value, bool) or not isinstance(value, str):
        raise PatternLabDataError(f"{field}: expected a string, got {type(value).__name__}.")
    if value.count("_") != 1:
        raise PatternLabDataError(
            f"{field}: expected exactly one '_' delimiter in <VENUE>_<CONTRACT>, got {value!r}."
        )
    venue, contract = value.split("_", 1)
    return build_instrument_id(venue, contract, where=field)


def instrument_file_name(instrument_id: str) -> str:
    """Return the stable per-instrument file name, without any date component."""
    return f"{normalize_instrument_id(instrument_id)}_{BASE_TIMEFRAME_MINUTES}m.parquet"


def instrument_relative_file(instrument_id: str) -> str:
    """Return the manifest-relative POSIX path for an instrument's 5m file."""
    return f"{OHLCV_DIR}/{instrument_file_name(instrument_id)}"


def validate_relative_file(value: Any, field: str = "file") -> str:
    """Validate a manifest-relative POSIX path under ``ohlcv/``."""
    if isinstance(value, bool) or not isinstance(value, str):
        raise PatternLabDataError(f"{field}: expected a string, got {type(value).__name__}.")
    if not value or value != value.strip():
        raise PatternLabDataError(f"{field}: must be a nonblank path without surrounding whitespace, got {value!r}.")
    if "\\" in value:
        raise PatternLabDataError(f"{field}: backslashes are not valid in a POSIX pack path, got {value!r}.")
    if value.startswith("/") or ":" in value:
        raise PatternLabDataError(f"{field}: must be a relative path, got {value!r}.")
    parts = PurePosixPath(value).parts
    if len(parts) != 2 or parts[0] != OHLCV_DIR:
        raise PatternLabDataError(f"{field}: must be '{OHLCV_DIR}/<name>.parquet', got {value!r}.")
    if not _FILE_NAME_RE.fullmatch(parts[1]):
        raise PatternLabDataError(
            f"{field}: file name must match ^[A-Z0-9][A-Za-z0-9._-]*\\.parquet$, got {parts[1]!r}."
        )
    return value


def resolve_pack_path(data_root: Path, relative: str, field: str = "file") -> Path:
    """Resolve a manifest-relative path, rejecting symlink escapes from the root."""
    validate_relative_file(relative, field)
    root = Path(data_root).resolve()
    target = (root / relative).resolve()
    if target != root and not target.is_relative_to(root):
        raise PatternLabDataError(f"{field}: {relative!r} resolves outside the data root {root}.")
    return target


def file_sha256(path: Path) -> str:
    """Return the lowercase SHA-256 digest of a file read in bounded chunks."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------
# scalar helpers
# --------------------------------------------------------------------------

def require_int(value: Any, field: str, *, minimum: int | None = None) -> int:
    """Return an exact integer; booleans and floats are rejected."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise PatternLabDataError(f"{field}: expected an integer, got {type(value).__name__}.")
    if minimum is not None and value < minimum:
        raise PatternLabDataError(f"{field}: must be >= {minimum}, got {value}.")
    return value


def require_text(value: Any, field: str) -> str:
    """Return a nonblank string."""
    if isinstance(value, bool) or not isinstance(value, str) or not value.strip():
        raise PatternLabDataError(f"{field}: expected a nonblank string, got {value!r}.")
    return value


def require_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise PatternLabDataError(f"{field}: expected a boolean, got {type(value).__name__}.")
    return value


def require_optional_text(value: Any, field: str) -> str | None:
    return None if value is None else require_text(value, field)


def require_currency(value: Any, field: str) -> str:
    text = require_text(value, field)
    if not _CURRENCY_RE.fullmatch(text):
        raise PatternLabDataError(f"{field}: expected an uppercase ASCII currency code, got {value!r}.")
    return text


def require_sha256(value: Any, field: str) -> str:
    text = require_text(value, field)
    if not _SHA256_RE.fullmatch(text):
        raise PatternLabDataError(f"{field}: expected 64 lowercase hex characters, got {value!r}.")
    return text


def parse_utc(value: Any, field: str) -> datetime:
    """Normalize an aware datetime or explicit-offset ISO string to UTC."""
    if isinstance(value, bool) or isinstance(value, (int, float)):
        raise PatternLabDataError(f"{field}: expected an ISO-8601 string or datetime with an explicit offset.")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise PatternLabDataError(f"{field}: expected a nonblank timestamp.")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise PatternLabDataError(f"{field}: not an ISO-8601 timestamp ({value!r}): {exc}") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise PatternLabDataError(
            f"{field}: expected an ISO-8601 string or datetime, got {type(value).__name__}."
        )
    if parsed != parsed:  # pandas NaT and any other not-a-time value
        raise PatternLabDataError(f"{field}: not-a-time values are not accepted.")
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise PatternLabDataError(f"{field}: naive timestamps are rejected; supply an explicit UTC offset.")
    return parsed.astimezone(timezone.utc)


def format_utc(value: Any, field: str = "timestamp") -> str:
    """Render a UTC timestamp canonically, with milliseconds only when needed."""
    moment = parse_utc(value, field)
    if moment.microsecond % 1000:
        raise PatternLabDataError(f"{field}: sub-millisecond precision is not supported.")
    if moment.microsecond:
        return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def to_epoch_ms(value: Any, field: str = "timestamp") -> int:
    """Return exact UTC epoch milliseconds without float rounding."""
    moment = parse_utc(value, field)
    if moment.microsecond % 1000:
        raise PatternLabDataError(f"{field}: sub-millisecond precision is not supported.")
    return (moment - _EPOCH) // timedelta(milliseconds=1)


def from_epoch_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def format_epoch_ms(value: int) -> str:
    return format_utc(from_epoch_ms(value))


def normalize_timeframe_minutes(value: Any, field: str = "timeframe_minutes") -> int:
    """Accept positive integer multiples of the 5-minute base timeframe."""
    minutes = require_int(value, field, minimum=1)
    if minutes % BASE_TIMEFRAME_MINUTES:
        raise PatternLabDataError(
            f"{field}: must be a positive multiple of {BASE_TIMEFRAME_MINUTES} minutes, got {minutes}."
        )
    return minutes


def require_aligned(epoch_ms: int, step_ms: int, field: str) -> int:
    if epoch_ms % step_ms:
        raise PatternLabDataError(
            f"{field}: {format_epoch_ms(epoch_ms)} is not aligned to the {step_ms // 60_000}m UTC epoch grid."
        )
    return epoch_ms


def normalize_roles(value: Any, field: str = "roles") -> list[str]:
    """Validate a role set against the legal combinations and return it sorted."""
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise PatternLabDataError(f"{field}: expected a list of roles, got {type(value).__name__}.")
    roles: list[str] = []
    for index, role in enumerate(value):
        text = require_text(role, f"{field}[{index}]")
        if text not in KNOWN_ROLES:
            raise PatternLabDataError(f"{field}[{index}]: unknown role {text!r}; known roles are {list(KNOWN_ROLES)}.")
        if text in roles:
            raise PatternLabDataError(f"{field}: duplicate role {text!r}.")
        roles.append(text)
    if frozenset(roles) not in LEGAL_ROLE_SETS:
        legal = ", ".join("{" + ", ".join(sorted(item)) + "}" for item in LEGAL_ROLE_SETS)
        raise PatternLabDataError(f"{field}: illegal role set {sorted(roles)}; legal sets are {legal}.")
    return sorted(roles)


# --------------------------------------------------------------------------
# strict JSON
# --------------------------------------------------------------------------

def _reject_duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in pairs:
        if key in result:
            raise PatternLabDataError(f"duplicate JSON key {key!r}.")
        result[key] = item
    return result


def _reject_constant(name: str) -> Any:
    raise PatternLabDataError(f"{name} is not valid strict JSON.")


def loads_strict(text: str, *, source: str) -> Any:
    """Parse strict JSON, rejecting duplicate keys and NaN/Infinity constants."""
    try:
        return json.loads(text, object_pairs_hook=_reject_duplicate_keys, parse_constant=_reject_constant)
    except PatternLabDataError as exc:
        raise PatternLabDataError(f"{source}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise PatternLabDataError(f"{source}: invalid JSON ({exc}).") from exc


def read_json_file(path: Path, *, source: str | None = None) -> Any:
    path = Path(path)
    label = source or str(path)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise PatternLabDataError(f"{label}: file not found.") from exc
    except UnicodeDecodeError as exc:
        raise PatternLabDataError(f"{label}: file is not valid UTF-8 ({exc}).") from exc
    return loads_strict(text, source=label)


def dumps_json(payload: Any) -> str:
    """Single strict JSON serializer used for every Pattern Lab text output."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)


def fsync_path(path: Path) -> None:
    """Flush one already-closed file's contents to stable storage."""
    handle = os.open(path, os.O_RDWR)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def fsync_directory(path: Path) -> None:
    """Flush a directory entry where the platform supports it.

    POSIX hosts flush the containing directory so a completed rename survives a
    process or host interruption.  Windows offers no supported directory flush,
    so this is a documented no-op there: interruption recovery on Windows relies
    on the retained journal, not on a flushed directory entry.
    """
    if os.name != "posix":
        return
    handle = os.open(path, os.O_RDONLY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def write_text_atomic(target: Path, text: str) -> None:
    """Write UTF-8 text durably through a sibling temporary file and a replace.

    The temporary is flushed and fsynced **before** the replace; the containing
    directory is flushed **after** it, so a crash leaves either the old file or
    the complete new one.
    """
    target = Path(target)
    temporary = target.with_name(f".{target.name}.tmp")
    payload = text.encode("utf-8")
    with open(temporary, "wb") as handle:
        handle.write(payload)
        handle.flush()
    fsync_path(temporary)
    os.replace(temporary, target)
    fsync_directory(target.parent)


# --------------------------------------------------------------------------
# manifest construction
# --------------------------------------------------------------------------

def build_universe(
    *,
    selection_source: str | None = None,
    selection_date: str | None = None,
    historical_membership: str = "unknown",
    notes: str | None = None,
) -> dict[str, Any]:
    """Build the universe provenance block; an unknown selection date stays null."""
    if historical_membership not in MEMBERSHIP_STATES:
        raise PatternLabDataError(
            f"universe.historical_membership: expected one of {list(MEMBERSHIP_STATES)}, got {historical_membership!r}."
        )
    if selection_date is not None and selection_source is None:
        raise PatternLabDataError(
            "universe.selection_date: a selection date requires a selection source; do not fabricate one."
        )
    return {
        "selection_source": require_optional_text(selection_source, "universe.selection_source"),
        "selection_date": require_optional_text(selection_date, "universe.selection_date"),
        "historical_membership": historical_membership,
        "notes": require_optional_text(notes, "universe.notes"),
    }


def build_source_metadata(
    *,
    input_format: str,
    input_dtype: str,
    source_reference: str,
    volume_unit_evidence: str,
    source_hash: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "input_format": require_text(input_format, "source.input_format"),
        "input_dtype": require_text(input_dtype, "source.input_dtype"),
        "source_hash": None if source_hash is None else require_sha256(source_hash, "source.source_hash"),
        "source_reference": require_text(source_reference, "source.source_reference"),
        "volume_unit_evidence": require_text(volume_unit_evidence, "source.volume_unit_evidence"),
    }
    payload.update(dict(extra or {}))
    return payload


def build_verification(
    *,
    volume_quote_verified: bool,
    volume_quote_evidence: str,
    closed_before_utc: Any = None,
    closure_evidence: str | None = None,
    closure_source: str | None = None,
) -> dict[str, Any]:
    """Build the verification block; closure evidence is all-or-nothing."""
    payload = {
        "volume_quote_verified": require_bool(volume_quote_verified, "verification.volume_quote_verified"),
        "volume_quote_evidence": require_text(volume_quote_evidence, "verification.volume_quote_evidence"),
        "closed_before_utc": None,
        "closure_evidence": None,
        "closure_source": None,
    }
    if closed_before_utc is None:
        if closure_evidence is not None or closure_source is not None:
            raise PatternLabDataError(
                "verification: closure evidence requires a non-null closed_before_utc."
            )
        return payload
    cutoff = to_epoch_ms(closed_before_utc, "verification.closed_before_utc")
    require_aligned(cutoff, BASE_STEP_MS, "verification.closed_before_utc")
    payload["closed_before_utc"] = format_epoch_ms(cutoff)
    payload["closure_evidence"] = require_text(closure_evidence, "verification.closure_evidence")
    payload["closure_source"] = require_text(closure_source, "verification.closure_source")
    return payload


def build_instrument_entry(
    *,
    symbol: str,
    venue: str,
    contract: str,
    quote_currency: str,
    roles: Sequence[str],
    row_count: int,
    first_open_ms: int,
    last_open_ms: int,
    missing_bar_count: int,
    sha256: str,
    source: Mapping[str, Any],
    verification: Mapping[str, Any],
    instrument_rules: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one manifest instrument entry from already-validated file facts."""
    instrument_id = build_instrument_id(venue, contract)
    entry = {
        "instrument_id": instrument_id,
        "symbol": require_text(symbol, "symbol"),
        "venue": normalize_id_part(venue, "venue"),
        "contract": normalize_id_part(contract, "contract"),
        "quote_currency": require_currency(quote_currency, "quote_currency"),
        "roles": normalize_roles(roles),
        "file": instrument_relative_file(instrument_id),
        "row_count": require_int(row_count, "row_count", minimum=1),
        "first_open_utc": format_epoch_ms(first_open_ms),
        "last_open_utc": format_epoch_ms(last_open_ms),
        "coverage_end_utc": format_epoch_ms(last_open_ms + BASE_STEP_MS),
        "missing_bar_count": require_int(missing_bar_count, "missing_bar_count", minimum=0),
        "sha256": require_sha256(sha256, "sha256"),
        "source": dict(source),
        "verification": dict(verification),
    }
    if instrument_rules is not None:
        if not isinstance(instrument_rules, Mapping):
            raise PatternLabDataError("instrument_rules: expected a metadata object.")
        entry["instrument_rules"] = dict(instrument_rules)
    return entry


def build_manifest(
    *,
    instruments: Sequence[Mapping[str, Any]],
    universe: Mapping[str, Any],
    generated_utc: Any,
    revision: int = 1,
    state: str = "ready",
    collector: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build and validate a complete manifest document.

    ``collector`` marks the pack as collector-managed: its roster, managed start
    and last request are validated, and every instrument entry must then carry
    the versioned v1 rule object.  Archival manifests without it stay valid and
    unmanaged.
    """
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "revision": require_int(revision, "revision", minimum=1),
        "state": state,
        "generated_utc": format_utc(generated_utc, "generated_utc"),
        "base_timeframe_minutes": BASE_TIMEFRAME_MINUTES,
        "volume_unit": VOLUME_UNIT,
        "universe": dict(universe),
        "instruments": [dict(entry) for entry in instruments],
    }
    if collector is not None:
        manifest["collector"] = dict(collector)
    return validate_manifest(manifest)


# --------------------------------------------------------------------------
# manifest validation
# --------------------------------------------------------------------------

def _require_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PatternLabDataError(f"{field}: expected an object, got {type(value).__name__}.")
    return dict(value)


def _validate_universe(raw: Any) -> dict[str, Any]:
    universe = _require_mapping(raw, "universe")
    membership = universe.get("historical_membership")
    if membership not in MEMBERSHIP_STATES:
        raise PatternLabDataError(
            f"universe.historical_membership: expected one of {list(MEMBERSHIP_STATES)}, got {membership!r}."
        )
    selection_source = require_optional_text(universe.get("selection_source"), "universe.selection_source")
    selection_date = require_optional_text(universe.get("selection_date"), "universe.selection_date")
    if selection_date is not None and selection_source is None:
        raise PatternLabDataError("universe.selection_date: recorded without a selection source.")
    universe["selection_source"] = selection_source
    universe["selection_date"] = selection_date
    universe["notes"] = require_optional_text(universe.get("notes"), "universe.notes")
    return universe


def _validate_source(raw: Any, where: str) -> dict[str, Any]:
    source = _require_mapping(raw, where)
    require_text(source.get("input_format"), f"{where}.input_format")
    require_text(source.get("input_dtype"), f"{where}.input_dtype")
    require_text(source.get("source_reference"), f"{where}.source_reference")
    require_text(source.get("volume_unit_evidence"), f"{where}.volume_unit_evidence")
    digest = source.get("source_hash")
    if digest is not None:
        require_sha256(digest, f"{where}.source_hash")
    return source


def _validate_verification(raw: Any, where: str) -> dict[str, Any]:
    verification = _require_mapping(raw, where)
    require_bool(verification.get("volume_quote_verified"), f"{where}.volume_quote_verified")
    require_text(verification.get("volume_quote_evidence"), f"{where}.volume_quote_evidence")
    cutoff = verification.get("closed_before_utc")
    evidence = verification.get("closure_evidence")
    origin = verification.get("closure_source")
    if cutoff is None:
        if evidence is not None or origin is not None:
            raise PatternLabDataError(
                f"{where}: closure evidence is recorded without a closed_before_utc cutoff."
            )
    else:
        cutoff_ms = to_epoch_ms(cutoff, f"{where}.closed_before_utc")
        require_aligned(cutoff_ms, BASE_STEP_MS, f"{where}.closed_before_utc")
        require_text(evidence, f"{where}.closure_evidence")
        require_text(origin, f"{where}.closure_source")
        verification["closed_before_utc"] = format_epoch_ms(cutoff_ms)
    return verification


def require_published_verification(raw: Any, where: str) -> dict[str, Any]:
    """Validate verification metadata under the stricter new-pack publication policy.

    Schema validation deliberately recognizes unverified quote volume so that a
    future writer's state can be read and tested, but a Pattern Lab publisher must
    never emit it.
    """
    verification = _validate_verification(raw, where)
    if verification["volume_quote_verified"] is not True:
        raise PatternLabDataError(
            f"{where}.volume_quote_verified: publication requires verified quote-volume units; "
            "unproven units are rejected rather than published."
        )
    return verification


# --------------------------------------------------------------------------
# collector provenance and instrument rules (populated by the M1b collector)
# --------------------------------------------------------------------------

def require_decimal_text(value: Any, field: str, *, allow_zero: bool = False) -> str:
    """Validate a finite decimal quantity string, preserving its source spelling."""
    text = require_text(value, field)
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise PatternLabDataError(f"{field}: expected a decimal number, got {value!r}.") from exc
    if not number.is_finite():
        raise PatternLabDataError(f"{field}: expected a finite decimal number, got {value!r}.")
    if number < 0 or (number == 0 and not allow_zero):
        raise PatternLabDataError(
            f"{field}: expected a {'nonnegative' if allow_zero else 'positive'} decimal, got {value!r}."
        )
    return text


def validate_roster_entries(raw: Any, where: str = "collector.roster") -> list[dict[str, Any]]:
    """Validate the closed six-key roster entries and return them canonically sorted."""
    if isinstance(raw, (str, bytes)) or not isinstance(raw, (list, tuple)):
        raise PatternLabDataError(f"{where}: expected a list of roster entries, got {type(raw).__name__}.")
    if not raw:
        raise PatternLabDataError(f"{where}: at least one roster entry is required.")
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        position = f"{where}[{index}]"
        entry = _require_mapping(item, position)
        missing = sorted(set(ROSTER_KEYS) - set(entry))
        extra = sorted(set(entry) - set(ROSTER_KEYS))
        if missing or extra:
            raise PatternLabDataError(
                f"{position}: the roster entry is closed; missing keys {missing}, unexpected keys {extra}."
            )
        venue = normalize_id_part(entry["venue"], f"{position}.venue")
        contract = normalize_id_part(entry["contract"], f"{position}.contract")
        instrument_id = normalize_instrument_id(entry["instrument_id"], f"{position}.instrument_id")
        if instrument_id != f"{venue}_{contract}":
            raise PatternLabDataError(
                f"{position}.instrument_id: {instrument_id!r} does not match venue/contract "
                f"{venue}_{contract}."
            )
        if instrument_id in seen:
            raise PatternLabDataError(f"{where}: duplicate instrument {instrument_id!r}.")
        seen.add(instrument_id)
        symbol = require_text(entry["symbol"], f"{position}.symbol")
        if not symbol.isascii() or symbol != symbol.strip() or any(ch.isspace() for ch in symbol):
            raise PatternLabDataError(
                f"{position}.symbol: expected ASCII text without whitespace, got {symbol!r}."
            )
        entries.append(
            {
                "contract": contract,
                "instrument_id": instrument_id,
                "quote_currency": require_currency(entry["quote_currency"], f"{position}.quote_currency"),
                "roles": normalize_roles(entry["roles"], f"{position}.roles"),
                "symbol": symbol,
                "venue": venue,
            }
        )
    ordered = sorted(entries, key=lambda item: item["instrument_id"])
    if [item["instrument_id"] for item in entries] != [item["instrument_id"] for item in ordered]:
        raise PatternLabDataError(f"{where}: entries must be sorted by instrument_id.")
    return ordered


def canonical_roster_bytes(roster: Sequence[Mapping[str, Any]]) -> bytes:
    """Return the exact bytes hashed into ``collector.roster_sha256``.

    Only the roster's semantic identity is hashed: no universe object, no dates,
    no paths and no physical file formatting.
    """
    return json.dumps(
        [dict(entry) for entry in roster],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def roster_sha256(roster: Sequence[Mapping[str, Any]]) -> str:
    """Return the canonical SHA-256 identity of a validated roster."""
    return hashlib.sha256(canonical_roster_bytes(roster)).hexdigest()


def validate_instrument_rules(raw: Any, where: str) -> dict[str, Any]:
    """Validate the closed v1 instrument-rule object of a collector-managed entry.

    These are current exchange metadata, not reconstructed historical rules.  M4
    owns their interpretation and enforcement; nothing here enables simulation.
    """
    rules = _require_mapping(raw, where)
    version = require_int(rules.get("schema_version"), f"{where}.schema_version")
    if version != INSTRUMENT_RULES_SCHEMA_VERSION:
        raise PatternLabDataError(
            f"{where}.schema_version: unsupported version {version}; this build writes "
            f"{INSTRUMENT_RULES_SCHEMA_VERSION}."
        )
    raw_fields = _require_mapping(rules.get("raw_contract_fields"), f"{where}.raw_contract_fields")
    for key, value in raw_fields.items():
        if value is not None and not isinstance(value, str):
            raise PatternLabDataError(
                f"{where}.raw_contract_fields.{key}: source fields are retained as strings or "
                f"explicit nulls, got {type(value).__name__}."
            )
    contract_type = rules.get("contract_type")
    if contract_type not in CONTRACT_TYPES:
        raise PatternLabDataError(
            f"{where}.contract_type: expected one of {list(CONTRACT_TYPES)}, got {contract_type!r}."
        )
    status = rules.get("trading_status")
    if status not in TRADING_STATUSES:
        raise PatternLabDataError(
            f"{where}.trading_status: expected the original source status, one of "
            f"{list(TRADING_STATUSES)}, got {status!r}."
        )
    base = require_currency(rules.get("base_currency"), f"{where}.base_currency")
    quote = require_currency(rules.get("quote_currency"), f"{where}.quote_currency")
    settlement = require_currency(rules.get("settlement_currency"), f"{where}.settlement_currency")
    if quote != "USDT" or settlement != "USDT":
        raise PatternLabDataError(
            f"{where}: v1 stores only USDT-quoted, USDT-settled linear contracts; got quote "
            f"{quote!r} and settlement {settlement!r}."
        )
    unit = require_text(rules.get("quantity_unit"), f"{where}.quantity_unit")
    if unit not in ("contracts", base):
        raise PatternLabDataError(
            f"{where}.quantity_unit: expected 'contracts' or the base currency {base!r}, got {unit!r}."
        )
    notional = rules.get("minimum_notional")
    listed = rules.get("listed_at_utc")
    validated = {
        "schema_version": version,
        "source_reference": require_text(rules.get("source_reference"), f"{where}.source_reference"),
        "as_of_utc": format_utc(rules.get("as_of_utc"), f"{where}.as_of_utc"),
        "contract_type": contract_type,
        "base_currency": base,
        "quote_currency": quote,
        "settlement_currency": settlement,
        "quantity_unit": unit,
        "quantity_step": require_decimal_text(rules.get("quantity_step"), f"{where}.quantity_step"),
        "minimum_quantity": require_decimal_text(rules.get("minimum_quantity"), f"{where}.minimum_quantity"),
        "price_tick": require_decimal_text(rules.get("price_tick"), f"{where}.price_tick"),
        "minimum_notional": None
        if notional is None
        else require_decimal_text(notional, f"{where}.minimum_notional", allow_zero=True),
        "listed_at_utc": None if listed is None else format_utc(listed, f"{where}.listed_at_utc"),
        "trading_status": status,
        "raw_contract_fields": dict(raw_fields),
    }
    extra = sorted(set(rules) - set(validated))
    if extra:
        raise PatternLabDataError(f"{where}: the v1 rule object is closed; unexpected keys {extra}.")
    return validated


def validate_collector(
    raw: Any, *, instruments: Sequence[Mapping[str, Any]], where: str = "manifest.collector"
) -> dict[str, Any]:
    """Validate the closed collector provenance object of a managed pack."""
    collector = _require_mapping(raw, where)
    version = require_int(collector.get("schema_version"), f"{where}.schema_version")
    if version != COLLECTOR_SCHEMA_VERSION:
        raise PatternLabDataError(
            f"{where}.schema_version: unsupported collector version {version}; this build writes "
            f"{COLLECTOR_SCHEMA_VERSION}."
        )
    roster = validate_roster_entries(collector.get("roster"), f"{where}.roster")
    digest = require_sha256(collector.get("roster_sha256"), f"{where}.roster_sha256")
    expected = roster_sha256(roster)
    if digest != expected:
        raise PatternLabDataError(
            f"{where}.roster_sha256: {digest} does not match the canonical roster digest {expected}."
        )
    declared = {entry["instrument_id"]: entry["roles"] for entry in roster}
    published = {entry["instrument_id"]: entry["roles"] for entry in instruments}
    if declared != published:
        raise PatternLabDataError(
            f"{where}.roster: the roster must match the published instrument identities and roles "
            f"exactly; roster {sorted(declared)} versus manifest {sorted(published)}."
        )
    managed_start = require_aligned(
        to_epoch_ms(collector.get("managed_start_utc"), f"{where}.managed_start_utc"),
        BASE_STEP_MS,
        f"{where}.managed_start_utc",
    )
    request = _require_mapping(collector.get("last_request"), f"{where}.last_request")
    request_extra = sorted(set(request) - {"start_utc", "end_utc"})
    if request_extra:
        raise PatternLabDataError(
            f"{where}.last_request: the object is closed; unexpected keys {request_extra}."
        )
    start_ms = require_aligned(
        to_epoch_ms(request.get("start_utc"), f"{where}.last_request.start_utc"),
        BASE_STEP_MS,
        f"{where}.last_request.start_utc",
    )
    end_ms = require_aligned(
        to_epoch_ms(request.get("end_utc"), f"{where}.last_request.end_utc"),
        BASE_STEP_MS,
        f"{where}.last_request.end_utc",
    )
    if start_ms >= end_ms:
        raise PatternLabDataError(
            f"{where}.last_request: requires start_utc < end_utc, got "
            f"{format_epoch_ms(start_ms)} >= {format_epoch_ms(end_ms)}."
        )
    if start_ms != managed_start:
        raise PatternLabDataError(
            f"{where}.last_request.start_utc: must equal managed_start_utc "
            f"{format_epoch_ms(managed_start)}, got {format_epoch_ms(start_ms)}."
        )
    validated = {
        "schema_version": version,
        "roster": roster,
        "roster_sha256": digest,
        "managed_start_utc": format_epoch_ms(managed_start),
        "last_request": {"start_utc": format_epoch_ms(start_ms), "end_utc": format_epoch_ms(end_ms)},
        "operation_id": require_text(collector.get("operation_id"), f"{where}.operation_id"),
    }
    extra = sorted(set(collector) - set(validated))
    if extra:
        raise PatternLabDataError(f"{where}: the v1 collector object is closed; unexpected keys {extra}.")
    return validated


def _validate_instrument(raw: Any, index: int, *, managed: bool = False) -> dict[str, Any]:
    where = f"instruments[{index}]"
    entry = _require_mapping(raw, where)
    venue = normalize_id_part(entry.get("venue"), f"{where}.venue")
    contract = normalize_id_part(entry.get("contract"), f"{where}.contract")
    instrument_id = normalize_instrument_id(entry.get("instrument_id"), f"{where}.instrument_id")
    if instrument_id != f"{venue}_{contract}":
        raise PatternLabDataError(
            f"{where}.instrument_id: {instrument_id!r} does not match venue/contract {venue}_{contract}."
        )
    symbol = require_text(entry.get("symbol"), f"{where}.symbol")
    if not symbol.isascii() or symbol != symbol.strip() or any(ch.isspace() for ch in symbol):
        raise PatternLabDataError(f"{where}.symbol: expected ASCII text without whitespace, got {symbol!r}.")

    first_ms = require_aligned(
        to_epoch_ms(entry.get("first_open_utc"), f"{where}.first_open_utc"), BASE_STEP_MS, f"{where}.first_open_utc"
    )
    last_ms = require_aligned(
        to_epoch_ms(entry.get("last_open_utc"), f"{where}.last_open_utc"), BASE_STEP_MS, f"{where}.last_open_utc"
    )
    coverage_end_ms = require_aligned(
        to_epoch_ms(entry.get("coverage_end_utc"), f"{where}.coverage_end_utc"),
        BASE_STEP_MS,
        f"{where}.coverage_end_utc",
    )
    if last_ms < first_ms:
        raise PatternLabDataError(f"{where}: last_open_utc precedes first_open_utc.")
    if coverage_end_ms != last_ms + BASE_STEP_MS:
        raise PatternLabDataError(
            f"{where}.coverage_end_utc: must be last_open_utc plus {BASE_TIMEFRAME_MINUTES}m (exclusive end)."
        )

    row_count = require_int(entry.get("row_count"), f"{where}.row_count", minimum=1)
    missing = require_int(entry.get("missing_bar_count"), f"{where}.missing_bar_count", minimum=0)
    span = (last_ms - first_ms) // BASE_STEP_MS + 1
    if row_count + missing != span:
        raise PatternLabDataError(
            f"{where}: row_count {row_count} plus missing_bar_count {missing} does not match the "
            f"{span}-slot coverage span."
        )

    entry["instrument_id"] = instrument_id
    entry["venue"] = venue
    entry["contract"] = contract
    entry["first_open_utc"] = format_epoch_ms(first_ms)
    entry["last_open_utc"] = format_epoch_ms(last_ms)
    entry["coverage_end_utc"] = format_epoch_ms(coverage_end_ms)
    entry["quote_currency"] = require_currency(entry.get("quote_currency"), f"{where}.quote_currency")
    entry["roles"] = normalize_roles(entry.get("roles"), f"{where}.roles")
    entry["file"] = validate_relative_file(entry.get("file"), f"{where}.file")
    entry["sha256"] = require_sha256(entry.get("sha256"), f"{where}.sha256")
    entry["source"] = _validate_source(entry.get("source"), f"{where}.source")
    entry["verification"] = _validate_verification(entry.get("verification"), f"{where}.verification")
    if managed:
        # A collector-managed entry always carries the versioned v1 rule object;
        # M1a archival packs keep their existing opaque-mapping behavior.
        entry["instrument_rules"] = validate_instrument_rules(
            entry.get("instrument_rules"), f"{where}.instrument_rules"
        )
    elif "instrument_rules" in entry and entry["instrument_rules"] is not None:
        entry["instrument_rules"] = _require_mapping(entry["instrument_rules"], f"{where}.instrument_rules")
    return entry


def validate_manifest(raw: Any) -> dict[str, Any]:
    """Validate a manifest document and return a normalized copy.

    Unknown extra keys are retained; required fields are never inferred from
    their absence, and an unknown schema version fails clearly.
    """
    manifest = _require_mapping(raw, "manifest")
    version = manifest.get("schema_version")
    if isinstance(version, bool) or not isinstance(version, int):
        raise PatternLabDataError("manifest.schema_version: expected an integer.")
    if version != SCHEMA_VERSION:
        raise PatternLabDataError(
            f"manifest.schema_version: unsupported version {version}; this build reads version {SCHEMA_VERSION}."
        )
    manifest["revision"] = require_int(manifest.get("revision"), "manifest.revision", minimum=1)
    state = manifest.get("state")
    if state not in PACK_STATES:
        raise PatternLabDataError(f"manifest.state: expected one of {list(PACK_STATES)}, got {state!r}.")
    manifest["generated_utc"] = format_utc(manifest.get("generated_utc"), "manifest.generated_utc")
    base = require_int(manifest.get("base_timeframe_minutes"), "manifest.base_timeframe_minutes")
    if base != BASE_TIMEFRAME_MINUTES:
        raise PatternLabDataError(
            f"manifest.base_timeframe_minutes: only {BASE_TIMEFRAME_MINUTES} is stored in schema v1, got {base!r}."
        )
    manifest["base_timeframe_minutes"] = base
    unit = manifest.get("volume_unit")
    if unit != VOLUME_UNIT:
        raise PatternLabDataError(f"manifest.volume_unit: expected {VOLUME_UNIT!r}, got {unit!r}.")
    manifest["universe"] = _validate_universe(manifest.get("universe"))

    instruments = manifest.get("instruments")
    if not isinstance(instruments, list) or not instruments:
        raise PatternLabDataError("manifest.instruments: expected a nonempty list.")
    managed = manifest.get("collector") is not None
    validated: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_files: set[str] = set()
    for index, entry in enumerate(instruments):
        item = _validate_instrument(entry, index, managed=managed)
        if item["instrument_id"] in seen_ids:
            raise PatternLabDataError(f"manifest.instruments: duplicate instrument_id {item['instrument_id']!r}.")
        if item["file"] in seen_files:
            raise PatternLabDataError(f"manifest.instruments: duplicate file {item['file']!r}.")
        seen_ids.add(item["instrument_id"])
        seen_files.add(item["file"])
        validated.append(item)
    manifest["instruments"] = validated
    if managed:
        manifest["collector"] = validate_collector(manifest["collector"], instruments=validated)
    return manifest


def find_instrument(manifest: Mapping[str, Any], instrument_id: str) -> dict[str, Any]:
    """Return one validated instrument entry, or fail naming the instrument."""
    wanted = normalize_instrument_id(instrument_id)
    for entry in manifest["instruments"]:
        if entry["instrument_id"] == wanted:
            return entry
    available = ", ".join(entry["instrument_id"] for entry in manifest["instruments"])
    raise PatternLabDataError(f"instrument {wanted!r} is not declared in the manifest; available: {available}.")


# --------------------------------------------------------------------------
# pack-level metadata IO
# --------------------------------------------------------------------------

def manifest_path(data_root: Path) -> Path:
    return Path(data_root) / MANIFEST_NAME


def update_marker_path(data_root: Path) -> Path:
    return Path(data_root) / UPDATE_MARKER_NAME


def read_manifest(data_root: Path) -> dict[str, Any]:
    """Read and validate the manifest of an existing pack."""
    path = manifest_path(data_root)
    if not path.is_file():
        raise PatternLabDataError(
            f"{path}: no readable pack manifest; the pack is missing or its publication did not complete."
        )
    return validate_manifest(read_json_file(path, source=str(path)))


def write_manifest(data_root: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and atomically publish a manifest document."""
    validated = validate_manifest(manifest)
    write_text_atomic(manifest_path(data_root), dumps_json(validated) + "\n")
    return validated


def build_update_record(
    manifest: Mapping[str, Any], *, event: str, source: Mapping[str, Any] | None = None, note: str | None = None
) -> dict[str, Any]:
    """Build one update-history record derived from the published manifest."""
    return {
        "event": require_text(event, "event"),
        "revision": manifest["revision"],
        "state": manifest["state"],
        "recorded_utc": manifest["generated_utc"],
        "instrument_count": len(manifest["instruments"]),
        "instruments": [entry["instrument_id"] for entry in manifest["instruments"]],
        "source": dict(source or {}),
        "note": require_optional_text(note, "note"),
    }


def render_update_line(record: Mapping[str, Any]) -> str:
    """Render one JSON Lines update record, including its trailing newline."""
    return json.dumps(record, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"


def append_update_record(data_root: Path, record: Mapping[str, Any]) -> None:
    """Append one JSON Lines update record using explicit UTF-8 and newlines."""
    path = Path(data_root) / UPDATES_NAME
    with open(path, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(render_update_line(record))


def research_blockers(entry: Mapping[str, Any]) -> list[str]:
    """List the verification gaps that refuse every research read of an instrument."""
    blockers: list[str] = []
    verification = entry["verification"]
    if not verification["volume_quote_verified"]:
        blockers.append("quote-volume units are not verified")
    if verification["closed_before_utc"] is None:
        blockers.append("final-candle closure is unknown; no interval can be read")
    return blockers


def research_limitations(entry: Mapping[str, Any]) -> list[str]:
    """List every recorded limitation, including non-blocking coverage gaps."""
    limitations = research_blockers(entry)
    if entry["missing_bar_count"]:
        limitations.append(f"{entry['missing_bar_count']} missing 5m bars inside the declared coverage")
    return limitations


def _evidence_lines(entry: Mapping[str, Any]) -> list[str]:
    """Render one instrument's verification evidence, source references and limits."""
    source = entry["source"]
    verification = entry["verification"]
    verified = "yes" if verification["volume_quote_verified"] else "no"
    lines = [
        f"- Quote volume verified: {verified} \u2014 {verification['volume_quote_evidence']}",
        f"- Volume unit: {VOLUME_UNIT} in {entry['quote_currency']}",
    ]
    evidence_source = source.get("evidence_source")
    if isinstance(evidence_source, str) and evidence_source.strip():
        lines.append(f"- Evidence source: {evidence_source}")
    lines.append(f"- Input: {source['input_format']} ({source['input_dtype']})")
    lines.append(f"- Source reference: {source['source_reference']}")
    if source.get("source_hash"):
        lines.append(f"- Source SHA-256: `{source['source_hash']}`")
    cutoff = verification["closed_before_utc"]
    if cutoff is None:
        lines.append(
            "- Closed before UTC: unknown \u2014 no closure evidence was supplied, so every "
            "research read of this instrument is refused."
        )
    else:
        lines.append(
            f"- Closed before UTC: {cutoff} \u2014 {verification['closure_evidence']} "
            f"(source: {verification['closure_source']})"
        )
    limitations = research_limitations(entry)
    lines.append(f"- Research limitations: {'; '.join(limitations) if limitations else 'none'}")
    rules = entry.get("instrument_rules")
    if isinstance(rules, Mapping) and rules.get("schema_version") == INSTRUMENT_RULES_SCHEMA_VERSION:
        lines += [
            f"- Contract: {rules['contract_type']}, {rules['base_currency']}/{rules['quote_currency']}, "
            f"settled in {rules['settlement_currency']}, status {rules['trading_status']}",
            f"- Quantity unit: {rules['quantity_unit']}; step {rules['quantity_step']}, "
            f"minimum {rules['minimum_quantity']}, price tick {rules['price_tick']}, "
            f"minimum notional {rules['minimum_notional'] or 'unavailable'}",
            f"- Listed at UTC: {rules['listed_at_utc'] or 'unknown'} "
            f"(rules observed {rules['as_of_utc']} from {rules['source_reference']})",
        ]
    return lines


def tail_shortfall_bars(entry: Mapping[str, Any], requested_end_utc: Any) -> int:
    """Return how many 5m slots an instrument's actual tail falls short of a request."""
    requested = to_epoch_ms(requested_end_utc, "requested_end_utc")
    covered = to_epoch_ms(entry["coverage_end_utc"], f"{entry['instrument_id']}.coverage_end_utc")
    return max(0, (requested - covered) // BASE_STEP_MS)


def _collector_lines(manifest: Mapping[str, Any]) -> list[str]:
    """Render the collector section: managed request, roster identity and shortfalls."""
    collector = manifest["collector"]
    request = collector["last_request"]
    lines = [
        "",
        "## Collector-managed coverage",
        "",
        f"- Managed start (UTC): {collector['managed_start_utc']}",
        f"- Last requested interval: [{request['start_utc']}, {request['end_utc']})",
        f"- Roster entries: {len(collector['roster'])} (SHA-256 `{collector['roster_sha256']}`)",
        f"- Publishing operation: `{collector['operation_id']}`",
        "",
        "The requested end is the frozen request of the last publishing operation. A published",
        "pack does not mean the requested range is complete: each instrument's actual coverage",
        "is authoritative, and a short tail is reported below rather than hidden.",
        "",
        "| Instrument | Coverage end UTC | Tail shortfall (5m bars) |",
        "| --- | --- | --- |",
    ]
    for entry in manifest["instruments"]:
        shortfall = tail_shortfall_bars(entry, request["end_utc"])
        lines.append(f"| {entry['instrument_id']} | {entry['coverage_end_utc']} | {shortfall} |")
    return lines


def render_readme(manifest: Mapping[str, Any]) -> str:
    """Render the human-readable pack README from the manifest alone."""
    universe = manifest["universe"]
    lines = [
        "# Pattern Lab market-data pack",
        "",
        "Generated from `manifest.json`. Do not edit this file by hand: it is a rendered",
        "view of the manifest, not a second authority.",
        "",
        f"- Manifest schema version: {manifest['schema_version']}",
        f"- Revision: {manifest['revision']}",
        f"- State: {manifest['state']}",
        f"- Generated (UTC): {manifest['generated_utc']}",
        f"- Base timeframe: {manifest['base_timeframe_minutes']}m",
        f"- Volume unit: {manifest['volume_unit']} (true exchange quote turnover)",
        f"- Instruments: {len(manifest['instruments'])}",
        "",
        "## Universe provenance",
        "",
        f"- Selection source: {universe['selection_source'] or 'unknown'}",
        f"- Selection date: {universe['selection_date'] or 'unknown'}",
        f"- Historical (point-in-time) membership: {universe['historical_membership']}",
        f"- Notes: {universe['notes'] or 'none'}",
        "",
        "Membership is the currently selected roster. A later generation time neither erases",
        "prior use of this data nor reconstructs point-in-time historical membership.",
        "",
        "## Coverage",
        "",
        "Coverage end is exclusive. Missing bars are absent 5m slots inside the declared",
        "coverage; they are preserved as gaps and never filled.",
        "",
        "| Instrument | Symbol | Roles | Quote | Rows | First open UTC | Coverage end UTC | Missing 5m bars |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for entry in manifest["instruments"]:
        lines.append(
            "| {id} | {symbol} | {roles} | {quote} | {rows} | {first} | {end} | {missing} |".format(
                id=entry["instrument_id"],
                symbol=entry["symbol"],
                roles=", ".join(entry["roles"]),
                quote=entry["quote_currency"],
                rows=entry["row_count"],
                first=entry["first_open_utc"],
                end=entry["coverage_end_utc"],
                missing=entry["missing_bar_count"],
            )
        )
    if manifest.get("collector") is not None:
        lines += _collector_lines(manifest)
    lines += [
        "",
        "## Verification, evidence and source limitations",
        "",
        "A closure cutoff certifies that retained bars ending at or before it were closed",
        "when observed. It does not certify a gap-free history. Research reads require a",
        "verified quote-volume unit and a closure cutoff at or after the requested end.",
        "",
        "Recorded evidence is the assertion supplied by the operator who published or",
        "imported this pack. The legacy NPZ importer performs no exchange verification of",
        "its own; it copies the declared evidence into this pack's provenance.",
    ]
    for entry in manifest["instruments"]:
        lines += ["", f"### {entry['instrument_id']}", ""] + _evidence_lines(entry)
    lines += [
        "",
        "## Files",
        "",
        "| Instrument | File | SHA-256 |",
        "| --- | --- | --- |",
    ]
    for entry in manifest["instruments"]:
        lines.append(f"| {entry['instrument_id']} | `{entry['file']}` | `{entry['sha256']}` |")
    lines += [
        "",
        "See `tools/pattern_lab/README.md` for the schema, reader contract and commands.",
        "",
    ]
    return "\n".join(lines)
