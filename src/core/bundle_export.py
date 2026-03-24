from __future__ import annotations

import hashlib
import re
import subprocess
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from . import __version__ as MERLIN_VERSION

__all__ = ["build_lancelot_partial_bundle"]

_NUMERIC_TIMEFRAME_RE = re.compile(r"^[^_]*_([^,]+),\s*(\d+)\s")
_TOKEN_TIMEFRAME_RE = re.compile(r"^[^_]*_([^,]+),\s*(\d+[mhdwMHDW])\s")
_SWAP_SYMBOL_RE = re.compile(
    r"^(?P<base>[A-Z0-9]+?)(?P<quote>USDT|USDC|USD|BTC|ETH)(?:\.P)?$"
)


def _normalize_timeframe(value: str) -> str:
    lower = str(value or "").strip().lower()
    if not lower:
        raise ValueError("Failed to infer timeframe from csv_file_name.")
    if lower.endswith("m") or lower.endswith("h") or lower.endswith("d") or lower.endswith("w"):
        return lower
    minutes = int(lower)
    if minutes >= 1440 and minutes % 1440 == 0:
        return f"{minutes // 1440}d"
    if minutes >= 60 and minutes % 60 == 0:
        return f"{minutes // 60}h"
    return f"{minutes}m"


def _parse_csv_symbol_and_timeframe(csv_file_name: str) -> tuple[str, str]:
    name = Path(str(csv_file_name or "")).name
    if not name:
        raise ValueError("Study is missing csv_file_name; cannot export Bundle.")

    numeric_match = _NUMERIC_TIMEFRAME_RE.match(name)
    if numeric_match:
        return numeric_match.group(1).strip(), _normalize_timeframe(numeric_match.group(2))

    token_match = _TOKEN_TIMEFRAME_RE.match(name)
    if token_match:
        return token_match.group(1).strip(), _normalize_timeframe(token_match.group(2))

    raise ValueError("Failed to parse symbol/timeframe from csv_file_name.")


def _normalize_swap_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    match = _SWAP_SYMBOL_RE.fullmatch(raw)
    if not match:
        raise ValueError(f"Unsupported export symbol '{symbol}'.")
    base = match.group("base")
    quote = match.group("quote")
    return f"{base}/{quote}:{quote}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


@lru_cache(maxsize=1)
def _get_merlin_commit() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"

    commit = (result.stdout or "").strip()
    if result.returncode != 0 or not commit:
        return "unknown"
    return commit


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_strategy_version(study: Mapping[str, Any]) -> str:
    raw_version = str(study.get("strategy_version") or "").strip()
    if raw_version:
        return raw_version

    strategy_id = str(study.get("strategy_id") or "").strip()
    if not strategy_id:
        raise ValueError("Study is missing strategy_id; cannot export Bundle.")

    try:
        from strategies import get_strategy_config

        strategy_config = get_strategy_config(strategy_id)
    except Exception as exc:  # pragma: no cover - defensive fallback
        raise ValueError("Failed to resolve strategy version for export.") from exc

    version = str(strategy_config.get("version") or "").strip()
    if not version:
        raise ValueError("Strategy version is missing; cannot export Bundle.")
    return version


def build_lancelot_partial_bundle(
    *,
    study: Mapping[str, Any],
    params: Mapping[str, Any],
    trial_number: int,
    csv_path: str,
) -> dict[str, Any]:
    params_payload = dict(params or {})
    if not params_payload:
        raise ValueError("Selected result has no params to export.")

    strategy_id = str(study.get("strategy_id") or "").strip()
    if not strategy_id:
        raise ValueError("Study is missing strategy_id; cannot export Bundle.")

    csv_name = str(study.get("csv_file_name") or Path(csv_path).name).strip()
    raw_symbol, timeframe = _parse_csv_symbol_and_timeframe(csv_name)
    symbol = _normalize_swap_symbol(raw_symbol)

    warmup_bars_raw = study.get("warmup_bars")
    if warmup_bars_raw in (None, ""):
        config = study.get("config_json")
        if isinstance(config, dict):
            warmup_bars_raw = config.get("warmup_bars")
    warmup_bars = int(warmup_bars_raw or 1000)
    if warmup_bars <= 0:
        raise ValueError("warmup_bars must be greater than zero for export.")

    source_study_name = str(study.get("study_name") or "").strip()
    if not source_study_name:
        raise ValueError("Study name is missing; cannot export Bundle.")

    source_study_id = str(study.get("study_id") or "").strip()
    if not source_study_id:
        raise ValueError("Study ID is missing; cannot export Bundle.")

    return {
        "bundleSchemaVersion": 2,
        "strategyId": strategy_id,
        "strategyVersion": _resolve_strategy_version(study),
        "symbol": symbol,
        "timeframe": timeframe,
        "warmupBars": warmup_bars,
        "exportMode": "live",
        "params": params_payload,
        "source": {
            "studyId": source_study_id,
            "trialNumber": max(0, int(trial_number)),
            "exportedAt": _utc_now_iso(),
            "studyName": source_study_name,
            "merlinVersion": MERLIN_VERSION,
            "merlinCommit": _get_merlin_commit(),
            "dataFingerprint": _sha256_file(Path(csv_path)),
        },
    }
