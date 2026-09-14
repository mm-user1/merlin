"""Public exchange adapters for closed 5m quote-turnover candles.

Only unauthenticated REST endpoints are used: server time, public instrument
metadata and public candle history.  There are no credentials, no websockets, no
order placement, no proxy rotation and no silent fallback to another venue.

Every network call goes through an injectable transport and clock, so tests
exercise the real request/cursor construction and response decoding against
synthetic protocol fixtures without touching the network.  Importing this module
opens no socket and writes no file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from typing import Any, Callable, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request

import numpy as np

from . import PatternLabDataError
from .manifest import (
    BASE_STEP_MS,
    format_epoch_ms,
    format_utc,
    listing_instant_utc,
    require_text,
)

OKX_BASE_URL = "https://www.okx.com"
BYBIT_BASE_URL = "https://api.bybit.com"

OKX_PAGE_LIMIT = 300  # current documented maximum for history-candles (default 100)
BYBIT_PAGE_LIMIT = 1000  # current documented maximum for /v5/market/kline

DEFAULT_TIMEOUT_SECONDS = 20.0
# One request never costs more than this many attempts in total: HTTP status,
# transport and venue business codes all share the same budget.
MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 0.5
BACKOFF_CEILING_SECONDS = 8.0
# A documented, bounded wait budget: a single request never waits longer than
# this in total, however large a Retry-After header or backoff would be.
RETRY_WAIT_BUDGET_SECONDS = 60.0

DEFAULT_REQUESTS_PER_SECOND = 2.0
# Conservative ceiling well under the published public-endpoint allowances
# (OKX history-candles 20 requests / 2 s per IP, Bybit 600 requests / 5 s per IP).
MAX_REQUESTS_PER_SECOND = 10.0

# Publication-latency allowance subtracted from the exchange clock before the
# safe closed cutoff is floored.  This is our conservative choice, not a
# guarantee published by either venue.
CLOSURE_LAG_MS = 60_000

# A collector-only convenience resolved once to a concrete end; it never enters
# load_slice or a frozen research configuration.
LATEST_CLOSED = "latest-closed"

USER_AGENT = "merlin-pattern-lab/1 (+local research tooling)"

# Documented transient public-REST conditions that are retried; every other
# business code fails on its first response, so an invalid symbol, an expired
# request window or an access restriction never looks like empty history.
#
# OKX: 50011 rate limit reached, 50013 systems are busy, 50026 system error.
# 50113 is an invalid-signature configuration error and is deliberately absent.
OKX_RETRYABLE_CODES = frozenset({"50011", "50013", "50026"})
# Bybit UTA REST: 10006 too many visits, 10016 server error, 10018 IP rate limit.
# 10002 is a request-time-window error, and 10429 is a WebSocket-only code; HTTP
# 429 is already classified from the status line, so neither belongs here.
BYBIT_RETRYABLE_CODES = frozenset({10006, 10016, 10018})

# Transport failure classes. A timeout or a temporary connection failure may be
# retried; a TLS or configuration failure is permanent and fails immediately,
# so the two are never indistinguishable status-0 retries.
TRANSPORT_KINDS = ("timeout", "connection", "tls", "configuration")
TRANSIENT_TRANSPORT_KINDS = frozenset({"timeout", "connection"})
_TRANSPORT_LABELS = {
    "timeout": "the request timed out",
    "connection": "a temporary connection failure",
    "tls": "a permanent TLS or certificate failure",
    "configuration": "a permanent transport configuration failure",
}

PROGRESS_PAGE_INTERVAL = 25

OPTION_KEYS = ("bybit_rps", "max_attempts", "okx_rps", "timeout_seconds")


def source_error(message: str, *, error_code: str = "source_failure") -> PatternLabDataError:
    """Return the error used for every source, protocol or transport failure."""
    return PatternLabDataError(message, error_code=error_code)


# --------------------------------------------------------------------------
# injectable transport and clock
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class HttpResponse:
    """One decoded HTTP response; ``status`` 0 marks a transport-level failure.

    ``error_kind`` names that failure's class, one of :data:`TRANSPORT_KINDS`.
    An unset or unknown kind is treated as a temporary connection failure.
    """

    status: int
    body: str
    headers: Mapping[str, str] = field(default_factory=dict)
    error: str | None = None
    error_kind: str | None = None

    def header(self, name: str) -> str | None:
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return None


def classify_transport_exception(exc: BaseException) -> str:
    """Return which of :data:`TRANSPORT_KINDS` one transport exception belongs to."""
    import socket
    import ssl

    if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, BaseException):
        return classify_transport_exception(exc.reason)
    if isinstance(exc, ssl.SSLError):  # includes SSLCertVerificationError
        return "tls"
    if isinstance(exc, TimeoutError):  # socket.timeout is an alias since 3.10
        return "timeout"
    if isinstance(exc, (ConnectionError, socket.gaierror, socket.herror)):
        return "connection"
    if isinstance(exc, OSError):
        return "connection"
    return "configuration"


def urllib_transport(url: str, timeout: float) -> HttpResponse:
    """Default transport: a plain GET with TLS verification left enabled."""
    request = urllib.request.Request(url, method="GET", headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return HttpResponse(
                status=int(response.status),
                body=response.read().decode("utf-8", errors="replace"),
                headers=dict(response.headers.items()),
            )
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:  # pragma: no cover - the body is optional diagnostics
            pass
        return HttpResponse(status=int(exc.code), body=body, headers=dict(exc.headers or {}))
    except Exception as exc:  # timeouts, DNS, TLS and configuration failures
        return HttpResponse(
            status=0,
            body="",
            error=f"{type(exc).__name__}: {exc}",
            error_kind=classify_transport_exception(exc),
        )


class SystemClock:
    """Wall clock, monotonic clock and sleeper, replaceable in tests."""

    def monotonic(self) -> float:
        import time

        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        import time

        if seconds > 0:
            time.sleep(seconds)

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc).replace(microsecond=0)


def normalize_rps(value: Any, field_name: str) -> float:
    """Validate one venue's requests-per-second pacing option."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PatternLabDataError(f"{field_name}: expected a number, got {type(value).__name__}.")
    rate = float(value)
    if not math.isfinite(rate) or rate <= 0.0:
        raise PatternLabDataError(f"{field_name}: must be a positive finite number, got {value!r}.")
    if rate > MAX_REQUESTS_PER_SECOND:
        raise PatternLabDataError(
            f"{field_name}: {rate} exceeds the conservative ceiling of "
            f"{MAX_REQUESTS_PER_SECOND} requests/second for these public endpoints."
        )
    return rate


def normalize_timeout(value: Any, field_name: str = "timeout_seconds") -> float:
    """Validate one finite positive per-request timeout in seconds."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PatternLabDataError(f"{field_name}: expected a number, got {type(value).__name__}.")
    timeout = float(value)
    if not math.isfinite(timeout) or timeout <= 0.0:
        raise PatternLabDataError(
            f"{field_name}: must be a positive finite number, got {value!r}."
        )
    return timeout


def normalize_max_attempts(value: Any, field_name: str = "max_attempts") -> int:
    """Validate the shared per-request attempt budget: an integer in [1, 5].

    The bound is enforced rather than clamped, so an out-of-range option is a
    rejected request instead of a silently different retry budget.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise PatternLabDataError(f"{field_name}: expected an integer, got {type(value).__name__}.")
    if not 1 <= value <= MAX_ATTEMPTS:
        raise PatternLabDataError(
            f"{field_name}: must be an integer in [1, {MAX_ATTEMPTS}], got {value}."
        )
    return value


def validate_http_options(raw: Any, *, where: str = "options") -> dict[str, Any]:
    """Validate the closed per-operation HTTP options frozen in the journal."""
    if not isinstance(raw, Mapping):
        raise PatternLabDataError(f"{where}: expected an object, got {type(raw).__name__}.")
    missing = sorted(set(OPTION_KEYS) - set(raw))
    extra = sorted(set(raw) - set(OPTION_KEYS))
    if missing or extra:
        raise PatternLabDataError(
            f"{where}: the options object is closed; missing keys {missing}, unexpected keys {extra}."
        )
    return {
        "okx_rps": normalize_rps(raw["okx_rps"], f"{where}.okx_rps"),
        "bybit_rps": normalize_rps(raw["bybit_rps"], f"{where}.bybit_rps"),
        "timeout_seconds": normalize_timeout(raw["timeout_seconds"], f"{where}.timeout_seconds"),
        "max_attempts": normalize_max_attempts(raw["max_attempts"], f"{where}.max_attempts"),
    }


@dataclass(frozen=True)
class _Attempt:
    """The outcome of one HTTP attempt: a payload, or a classified failure."""

    payload: Any = None
    ok: bool = False
    retryable: bool = False
    reason: str = ""


def _classify_attempt(response: HttpResponse, check) -> _Attempt:
    """Classify transport, HTTP status, JSON and venue business errors alike.

    One classifier feeds one attempt budget: an HTTP-200 response carrying a
    transient venue code is retried exactly like a 5xx, and a permanent code
    fails on its first response.
    """
    if response.status == 0:
        kind = response.error_kind if response.error_kind in TRANSPORT_KINDS else "connection"
        return _Attempt(
            retryable=kind in TRANSIENT_TRANSPORT_KINDS,
            reason=f"{_TRANSPORT_LABELS[kind]} ({response.error})",
        )
    if response.status == 429:
        return _Attempt(retryable=True, reason="HTTP 429 rate limit")
    if 500 <= response.status < 600:
        return _Attempt(retryable=True, reason=f"HTTP {response.status} from the venue")
    if response.status != 200:
        return _Attempt(
            reason=f"HTTP {response.status} from the venue: {response.body[:200]!r}"
        )
    try:
        payload = json.loads(response.body)
    except ValueError as exc:
        return _Attempt(reason=f"the response body is not valid JSON ({exc})")
    if check is not None:
        problem = check(payload)
        if problem is not None:
            retryable, reason = problem
            return _Attempt(retryable=retryable, reason=reason)
    return _Attempt(payload=payload, ok=True)


class HttpClient:
    """Paced, bounded-retry JSON client shared by both venue adapters."""

    def __init__(
        self,
        *,
        transport: Callable[[str, float], HttpResponse] | None = None,
        clock: Any | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = MAX_ATTEMPTS,
        rates: Mapping[str, float] | None = None,
    ):
        self.transport = transport or urllib_transport
        self.clock = clock or SystemClock()
        self.timeout = normalize_timeout(timeout)
        self.max_attempts = normalize_max_attempts(max_attempts)
        self.rates = {venue: float(rate) for venue, rate in (rates or {}).items()}
        self.request_count = 0
        self._last_request: dict[str, float] = {}

    def _pace(self, venue: str) -> None:
        rate = self.rates.get(venue, DEFAULT_REQUESTS_PER_SECOND)
        interval = 1.0 / rate
        previous = self._last_request.get(venue)
        now = self.clock.monotonic()
        if previous is not None:
            wait = previous + interval - now
            if wait > 0:
                self.clock.sleep(wait)
                now = self.clock.monotonic()
        self._last_request[venue] = now

    def get_json(
        self,
        url: str,
        params: Mapping[str, Any],
        *,
        venue: str,
        where: str,
        check: Callable[[Any], tuple[bool, str] | None] | None = None,
    ) -> Any:
        """Return decoded JSON, retrying only documented transient conditions.

        ``check`` is the venue's business-code classifier.  It shares this one
        attempt/backoff budget, so there is no nested retry loop multiplying the
        number of requests a single logical call can make.
        """
        target = f"{url}?{urllib.parse.urlencode(params)}" if params else url
        budget = RETRY_WAIT_BUDGET_SECONDS
        reason = "no attempt was made"
        attempt = 0
        for attempt in range(1, self.max_attempts + 1):
            self._pace(venue)
            self.request_count += 1
            response = self.transport(target, self.timeout)
            outcome = _classify_attempt(response, check)
            if outcome.ok:
                return outcome.payload
            reason = outcome.reason
            if not outcome.retryable or attempt == self.max_attempts:
                break
            wait = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_CEILING_SECONDS)
            hinted = _retry_after_seconds(response)
            if hinted is not None:
                wait = max(wait, hinted)
            wait = min(wait, budget)
            if wait <= 0:
                break
            budget -= wait
            self.clock.sleep(wait)
        raise source_error(
            f"{where}: {reason}; failed after {attempt} attempt(s) of at most "
            f"{self.max_attempts} (target {target})."
        )


def _retry_after_seconds(response: HttpResponse) -> float | None:
    raw = response.header("Retry-After")
    if raw is None:
        return None
    try:
        seconds = float(raw.strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return min(seconds, RETRY_WAIT_BUDGET_SECONDS)


# --------------------------------------------------------------------------
# shared parsing helpers
# --------------------------------------------------------------------------

def _require_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise source_error(f"{where}: expected a JSON object, got {type(value).__name__}.")
    return value


def _require_list(value: Any, where: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise source_error(f"{where}: expected a JSON array, got {type(value).__name__}.")
    return value


def _float(value: Any, where: str) -> float:
    """Parse one numeric string directly to float64; no float32 intermediate."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise source_error(f"{where}: expected a numeric string, got {type(value).__name__}.")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise source_error(f"{where}: {value!r} is not a number.") from exc
    if not math.isfinite(number):
        raise source_error(f"{where}: {value!r} is not finite.")
    return number


def _epoch_ms(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise source_error(f"{where}: expected an integer millisecond timestamp, got {value!r}.")
    try:
        stamp = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise source_error(f"{where}: {value!r} is not an integer millisecond timestamp.") from exc
    return stamp


def _optional_text(value: Any) -> str | None:
    """Return a nonblank source string, mapping an explicitly absent value to null."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    return value


def _decimal_text(value: Any, where: str, *, allow_zero: bool = False) -> str:
    """Validate a positive finite decimal quantity, preserving its source spelling."""
    text = require_text(value, where)
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise source_error(f"{where}: {value!r} is not a decimal number.") from exc
    if not number.is_finite():
        raise source_error(f"{where}: {value!r} is not finite.")
    if number < 0 or (number == 0 and not allow_zero):
        raise source_error(
            f"{where}: expected a {'nonnegative' if allow_zero else 'positive'} value, got {value!r}."
        )
    return text


def _listed_at_utc(raw: Any, where: str) -> str | None:
    """Return the canonical listing instant, or null when the source omits it.

    The conversion rule lives in :func:`manifest.listing_instant_utc` so the
    collector and manifest validation agree on exactly one interpretation.
    """
    return listing_instant_utc(raw, where)


def _sorted_unique_rows(
    rows: list[tuple[int, float, float, float, float, float]], where: str
) -> tuple[np.ndarray, np.ndarray]:
    """Sort downloaded rows stably; identical duplicates collapse, conflicts fail."""
    rows.sort(key=lambda item: item[0])
    stamps: list[int] = []
    values: list[tuple[float, float, float, float, float]] = []
    for row in rows:
        stamp, payload = row[0], row[1:]
        if stamps and stamps[-1] == stamp:
            if values[-1] != payload:
                raise source_error(
                    f"{where}: the source returned conflicting values for "
                    f"{format_epoch_ms(stamp)}: {values[-1]} then {payload}.",
                    error_code="source_conflict",
                )
            continue
        stamps.append(stamp)
        values.append(payload)
    stamp_array = np.asarray(stamps, dtype=np.int64)
    value_array = (
        np.asarray(values, dtype=np.float64)
        if values
        else np.zeros((0, 5), dtype=np.float64)
    )
    return stamp_array, value_array


# --------------------------------------------------------------------------
# venue adapters
# --------------------------------------------------------------------------

def _okx_business_problem(payload: Any) -> tuple[bool, str] | None:
    """Classify an OKX HTTP-200 envelope for the shared attempt budget."""
    if not isinstance(payload, Mapping):
        return False, f"OKX returned {type(payload).__name__}, not a JSON object"
    code = payload.get("code")
    if code == "0":
        return None
    retryable = isinstance(code, str) and code in OKX_RETRYABLE_CODES
    kind = "a documented transient condition" if retryable else "not a retryable condition"
    return retryable, f"OKX returned code {code!r} ({payload.get('msg')!r}), {kind}"


def _bybit_business_problem(payload: Any) -> tuple[bool, str] | None:
    """Classify a Bybit HTTP-200 envelope for the shared attempt budget."""
    if not isinstance(payload, Mapping):
        return False, f"Bybit returned {type(payload).__name__}, not a JSON object"
    code = payload.get("retCode")
    if code == 0 and not isinstance(code, bool):
        return None
    retryable = isinstance(code, int) and not isinstance(code, bool) and code in BYBIT_RETRYABLE_CODES
    kind = "a documented transient condition" if retryable else "not a retryable condition"
    return retryable, f"Bybit returned retCode {code!r} ({payload.get('retMsg')!r}), {kind}"


class OkxAdapter:
    """OKX USDT-margined linear SWAP contracts."""

    venue = "OKX"
    page_limit = OKX_PAGE_LIMIT
    candle_reference = "GET /api/v5/market/history-candles (bar=5m)"
    instrument_reference = "GET /api/v5/public/instruments (instType=SWAP)"
    time_reference = "GET /api/v5/public/time"
    quote_volume_evidence = (
        "OKX history-candles array index 7 is volCcyQuote, the quote-currency turnover of the bar."
    )

    def __init__(self, base_url: str = OKX_BASE_URL):
        self.base_url = base_url.rstrip("/")

    def _get(self, client: HttpClient, path: str, params: Mapping[str, Any], where: str) -> list[Any]:
        payload = client.get_json(
            f"{self.base_url}{path}",
            params,
            venue=self.venue,
            where=where,
            check=_okx_business_problem,
        )
        return list(_require_list(payload.get("data"), f"{where}.data"))

    def server_time_ms(self, client: HttpClient) -> int:
        rows = self._get(client, "/api/v5/public/time", {}, "OKX server time")
        if not rows:
            raise source_error("OKX server time: the response contained no data.")
        return _epoch_ms(_require_mapping(rows[0], "OKX server time[0]").get("ts"), "OKX server time.ts")

    def instrument_metadata(self, client: HttpClient, contract: str) -> dict[str, Any]:
        where = f"OKX instrument {contract}"
        rows = self._get(
            client,
            "/api/v5/public/instruments",
            {"instType": "SWAP", "instId": contract},
            where,
        )
        matched = [
            row for row in rows if _require_mapping(row, where).get("instId") == contract
        ]
        if not matched:
            raise source_error(
                f"{where}: the venue returned no SWAP instrument with this exact instId.",
                error_code="instrument_unavailable",
            )
        raw = _require_mapping(matched[0], where)
        return self._normalize_rules(raw, contract, where, client)

    def _normalize_rules(
        self, raw: Mapping[str, Any], contract: str, where: str, client: HttpClient
    ) -> dict[str, Any]:
        inst_type = raw.get("instType")
        contract_type = raw.get("ctType")
        settle = raw.get("settleCcy")
        if inst_type != "SWAP" or contract_type != "linear" or settle != "USDT":
            raise source_error(
                f"{where}: v1 supports only OKX USDT linear SWAP contracts; the venue reports "
                f"instType={inst_type!r}, ctType={contract_type!r}, settleCcy={settle!r}.",
                error_code="unsupported_product",
            )
        state = raw.get("state")
        if state != "live":
            raise source_error(
                f"{where}: instrument state is {state!r}; an available live contract is required.",
                error_code="instrument_unavailable",
            )
        base = require_text(raw.get("ctValCcy"), f"{where}.ctValCcy")
        rules = {
            "schema_version": 1,
            "source_reference": f"{self.instrument_reference} instId={contract}",
            "as_of_utc": _format_now(client),
            "contract_type": "linear_perpetual",
            "base_currency": base,
            "quote_currency": settle,
            "settlement_currency": settle,
            "quantity_unit": "contracts",
            "quantity_step": _decimal_text(raw.get("lotSz"), f"{where}.lotSz"),
            "minimum_quantity": _decimal_text(raw.get("minSz"), f"{where}.minSz"),
            "price_tick": _decimal_text(raw.get("tickSz"), f"{where}.tickSz"),
            "minimum_notional": None,
            "listed_at_utc": _listed_at_utc(raw.get("listTime"), f"{where}.listTime"),
            "trading_status": state,
            "raw_contract_fields": {
                "instType": inst_type,
                "ctType": contract_type,
                "settleCcy": settle,
                "ctVal": _decimal_text(raw.get("ctVal"), f"{where}.ctVal"),
                "ctValCcy": base,
                "ctMult": _optional_text(raw.get("ctMult")),
                "lotSz": require_text(raw.get("lotSz"), f"{where}.lotSz"),
                "minSz": require_text(raw.get("minSz"), f"{where}.minSz"),
                "tickSz": require_text(raw.get("tickSz"), f"{where}.tickSz"),
                "listTime": _optional_text(raw.get("listTime")),
                "state": state,
            },
        }
        return rules

    def fetch_candles(
        self,
        client: HttpClient,
        contract: str,
        *,
        start_ms: int,
        end_ms: int,
        limit: int | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Page backwards from ``end_ms`` with ``after`` until ``start_ms`` is reached."""
        where = f"OKX candles {contract}"
        page_limit = self.page_limit if limit is None else limit
        cursor = end_ms
        collected: list[tuple[int, float, float, float, float, float]] = []
        pages = 0
        while True:
            rows = self._get(
                client,
                "/api/v5/market/history-candles",
                {"instId": contract, "bar": "5m", "limit": page_limit, "after": cursor},
                f"{where} page {pages + 1}",
            )
            pages += 1
            if not rows:
                break  # a successful empty response establishes exhausted history
            oldest = None
            for index, row in enumerate(rows):
                position = f"{where} page {pages} row {index}"
                if not isinstance(row, list) or len(row) < 9:
                    raise source_error(f"{position}: expected a candle array with at least 9 fields.")
                stamp = _epoch_ms(row[0], f"{position}.timestamp")
                oldest = stamp if oldest is None else min(oldest, stamp)
                if row[8] != "1":
                    continue  # OKX confirm flag: only completed candles are retained
                if stamp < start_ms or stamp + BASE_STEP_MS > end_ms:
                    continue
                collected.append(
                    (
                        stamp,
                        _float(row[1], f"{position}.open"),
                        _float(row[2], f"{position}.high"),
                        _float(row[3], f"{position}.low"),
                        _float(row[4], f"{position}.close"),
                        _float(row[7], f"{position}.volCcyQuote"),
                    )
                )
            if oldest is None or oldest >= cursor:
                raise source_error(
                    f"{where}: pagination did not progress backwards past "
                    f"{format_epoch_ms(cursor)}; refusing to treat a repeated page as completed history.",
                    error_code="stalled_pagination",
                )
            cursor = oldest
            if oldest <= start_ms:
                break
            if progress is not None and pages % PROGRESS_PAGE_INTERVAL == 0:
                progress(f"{contract}: {pages} pages, back to {format_epoch_ms(oldest)}")
        return _sorted_unique_rows(collected, where)


class BybitAdapter:
    """Bybit USDT linear perpetual contracts."""

    venue = "BYBIT"
    page_limit = BYBIT_PAGE_LIMIT
    candle_reference = "GET /v5/market/kline (category=linear, interval=5)"
    instrument_reference = "GET /v5/market/instruments-info (category=linear)"
    time_reference = "GET /v5/market/time"
    quote_volume_evidence = (
        "Bybit linear kline array index 6 is turnover, the quote-coin turnover of the bar."
    )

    def __init__(self, base_url: str = BYBIT_BASE_URL):
        self.base_url = base_url.rstrip("/")

    def _get(self, client: HttpClient, path: str, params: Mapping[str, Any], where: str) -> Mapping[str, Any]:
        payload = client.get_json(
            f"{self.base_url}{path}",
            params,
            venue=self.venue,
            where=where,
            check=_bybit_business_problem,
        )
        return _require_mapping(payload.get("result"), f"{where}.result")

    def server_time_ms(self, client: HttpClient) -> int:
        result = self._get(client, "/v5/market/time", {}, "Bybit server time")
        nanos = _optional_text(result.get("timeNano"))
        if nanos is not None:
            return _epoch_ms(nanos, "Bybit server time.timeNano") // 1_000_000
        return _epoch_ms(result.get("timeSecond"), "Bybit server time.timeSecond") * 1000

    def instrument_metadata(self, client: HttpClient, contract: str) -> dict[str, Any]:
        where = f"Bybit instrument {contract}"
        result = self._get(
            client,
            "/v5/market/instruments-info",
            {"category": "linear", "symbol": contract},
            where,
        )
        if result.get("category") != "linear":
            raise source_error(f"{where}: the response category is {result.get('category')!r}, not 'linear'.")
        rows = [
            row
            for row in _require_list(result.get("list"), f"{where}.list")
            if _require_mapping(row, where).get("symbol") == contract
        ]
        if not rows:
            raise source_error(
                f"{where}: the venue returned no linear instrument with this exact symbol.",
                error_code="instrument_unavailable",
            )
        return self._normalize_rules(_require_mapping(rows[0], where), contract, where, client)

    def _normalize_rules(
        self, raw: Mapping[str, Any], contract: str, where: str, client: HttpClient
    ) -> dict[str, Any]:
        contract_type = raw.get("contractType")
        quote = raw.get("quoteCoin")
        settle = raw.get("settleCoin")
        if contract_type != "LinearPerpetual" or quote != "USDT" or settle != "USDT":
            raise source_error(
                f"{where}: v1 supports only Bybit USDT linear perpetual contracts; the venue reports "
                f"contractType={contract_type!r}, quoteCoin={quote!r}, settleCoin={settle!r}.",
                error_code="unsupported_product",
            )
        status = raw.get("status")
        if status != "Trading":
            raise source_error(
                f"{where}: instrument status is {status!r}; an available Trading contract is required.",
                error_code="instrument_unavailable",
            )
        base = require_text(raw.get("baseCoin"), f"{where}.baseCoin")
        lot = _require_mapping(raw.get("lotSizeFilter"), f"{where}.lotSizeFilter")
        price = _require_mapping(raw.get("priceFilter"), f"{where}.priceFilter")
        notional_raw = _optional_text(lot.get("minNotionalValue"))
        return {
            "schema_version": 1,
            "source_reference": f"{self.instrument_reference} symbol={contract}",
            "as_of_utc": _format_now(client),
            "contract_type": "linear_perpetual",
            "base_currency": base,
            "quote_currency": quote,
            "settlement_currency": settle,
            "quantity_unit": base,
            "quantity_step": _decimal_text(lot.get("qtyStep"), f"{where}.lotSizeFilter.qtyStep"),
            "minimum_quantity": _decimal_text(lot.get("minOrderQty"), f"{where}.lotSizeFilter.minOrderQty"),
            "price_tick": _decimal_text(price.get("tickSize"), f"{where}.priceFilter.tickSize"),
            "minimum_notional": None
            if notional_raw is None
            else _decimal_text(notional_raw, f"{where}.lotSizeFilter.minNotionalValue", allow_zero=True),
            "listed_at_utc": _listed_at_utc(raw.get("launchTime"), f"{where}.launchTime"),
            "trading_status": status,
            "raw_contract_fields": {
                "contractType": contract_type,
                "baseCoin": base,
                "quoteCoin": quote,
                "settleCoin": settle,
                "launchTime": _optional_text(raw.get("launchTime")),
                "status": status,
                "qtyStep": require_text(lot.get("qtyStep"), f"{where}.lotSizeFilter.qtyStep"),
                "minOrderQty": require_text(lot.get("minOrderQty"), f"{where}.lotSizeFilter.minOrderQty"),
                "minNotionalValue": notional_raw,
                "tickSize": require_text(price.get("tickSize"), f"{where}.priceFilter.tickSize"),
            },
        }

    def fetch_candles(
        self,
        client: HttpClient,
        contract: str,
        *,
        start_ms: int,
        end_ms: int,
        limit: int | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Page backwards using ``end=oldest-1`` until ``start_ms`` is reached."""
        where = f"Bybit candles {contract}"
        page_limit = self.page_limit if limit is None else limit
        cursor = end_ms - 1
        collected: list[tuple[int, float, float, float, float, float]] = []
        pages = 0
        while True:
            result = self._get(
                client,
                "/v5/market/kline",
                {
                    "category": "linear",
                    "symbol": contract,
                    "interval": "5",
                    "limit": page_limit,
                    "start": start_ms,
                    "end": cursor,
                },
                f"{where} page {pages + 1}",
            )
            pages += 1
            if result.get("symbol") != contract:
                raise source_error(
                    f"{where} page {pages}: the response symbol is {result.get('symbol')!r}, not {contract!r}."
                )
            rows = _require_list(result.get("list"), f"{where} page {pages}.list")
            if not rows:
                break  # a successful empty response establishes exhausted history
            oldest = None
            for index, row in enumerate(rows):
                position = f"{where} page {pages} row {index}"
                if not isinstance(row, list) or len(row) < 7:
                    raise source_error(f"{position}: expected a kline array with at least 7 fields.")
                stamp = _epoch_ms(row[0], f"{position}.startTime")
                oldest = stamp if oldest is None else min(oldest, stamp)
                if stamp < start_ms or stamp + BASE_STEP_MS > end_ms:
                    continue
                collected.append(
                    (
                        stamp,
                        _float(row[1], f"{position}.open"),
                        _float(row[2], f"{position}.high"),
                        _float(row[3], f"{position}.low"),
                        _float(row[4], f"{position}.close"),
                        _float(row[6], f"{position}.turnover"),
                    )
                )
            if oldest is None or oldest > cursor:
                raise source_error(
                    f"{where}: pagination did not progress backwards past "
                    f"{format_epoch_ms(cursor)}; refusing to treat a repeated page as completed history.",
                    error_code="stalled_pagination",
                )
            if oldest <= start_ms:
                break
            cursor = oldest - 1
            if progress is not None and pages % PROGRESS_PAGE_INTERVAL == 0:
                progress(f"{contract}: {pages} pages, back to {format_epoch_ms(oldest)}")
        return _sorted_unique_rows(collected, where)


ADAPTERS: dict[str, Any] = {"OKX": OkxAdapter(), "BYBIT": BybitAdapter()}


def adapter_for(venue: str):
    """Return the adapter for a supported venue, or fail naming the venue."""
    try:
        return ADAPTERS[venue]
    except KeyError as exc:
        raise PatternLabDataError(
            f"venue {venue!r} is not supported; v1 collects from {sorted(ADAPTERS)}.",
            error_code="unsupported_venue",
        ) from exc


def _format_now(client: HttpClient) -> str:
    """Return the actual metadata observation time, never a fabricated instant."""
    return format_utc(client.clock.now_utc(), "as_of_utc")


def probe_first_slot(adapter, client: HttpClient, contract: str, slot_ms: int) -> bool:
    """Return whether the exact requested first 5m slot is retrievable right now.

    This is one bounded page ending just after the slot, not a walk through years
    of history.  Metadata alone cannot prove candle retention depth, so this probe
    is the only evidence that a requested managed start is actually available.
    """
    stamps, _ = adapter.fetch_candles(
        client, contract, start_ms=slot_ms, end_ms=slot_ms + BASE_STEP_MS, limit=1
    )
    return bool(stamps.size) and int(stamps[0]) == slot_ms


def safe_closed_cutoff_ms(server_times_ms: Sequence[int]) -> int:
    """Freeze one common safe closed 5m boundary from the sampled server clocks."""
    if not server_times_ms:
        raise source_error("closure: at least one venue server time is required.")
    return ((min(int(value) for value in server_times_ms) - CLOSURE_LAG_MS) // BASE_STEP_MS) * BASE_STEP_MS
