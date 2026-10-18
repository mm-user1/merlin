"""Explicit columnar in-memory evidence. Disk codecs belong to T01B."""
import pyarrow as pa


def schema(required, optional=()):
    return pa.schema([pa.field(n, t, nullable=False) for n, t in required] +
                     [pa.field(n, t, nullable=True) for n, t in optional])

I, F, B, S = pa.int64(), pa.float64(), pa.bool_(), pa.string()
ATTEMPTS = schema([
    ("attemptId", I), ("eventId", S), ("side", I), ("signalIndex", I), ("decisionMs", I),
    ("intendedFillMs", I), ("anchorPrice", F), ("balance", F), ("reason", S),
    ("rawConflict", B), ("eligibleConflict", B), ("occupied", B), ("heldThisBar", B),
    ("filterKnown", B), ("filterAllowed", B), ("sourceIndex", I), ("sourceReason", I)], [
    ("observedAtMs", I), ("availableAtMs", I), ("validUntilMs", I),
    ("stop", F), ("target", F), ("riskDistance", F), ("quantity", F), ("lots", I),
    ("fillIndex", I), ("fillMs", I), ("fillPrice", F), ("notional", F),
    ("proposedFee", F), ("requiredLeverage", F), ("tradeId", I)])
TRADES = schema([
    ("tradeId", I), ("attemptId", I), ("eventId", S), ("side", I), ("signalIndex", I),
    ("decisionMs", I), ("entryIndex", I), ("entryMs", I), ("entryPrice", F),
    ("exitIndex", I), ("exitMs", I), ("exitBarOpenMs", I), ("exitBarEndMs", I),
    ("exitPrice", F), ("exitPhase", S), ("exitReason", S), ("holdingMs", I),
    ("quantity", F), ("lots", I), ("initialStop", F), ("activeStop", F),
    ("riskDistance", F), ("plannedCashRisk", F), ("entryFee", F), ("exitFee", F),
    ("grossPnl", F), ("netPnl", F), ("entryLeverage", F), ("balanceAfter", F),
    ("openingStop", B), ("openingTarget", B), ("explicitExitPending", B),
    ("expiryPending", B), ("ambiguous", B)], [("target", F)])
PROPOSALS = schema([
    ("tradeId", I), ("decisionIndex", I), ("decisionMs", I), ("effectiveMs", I),
    ("oldStop", F), ("newStop", F), ("known", B), ("status", S)], [("proposal", F)])
EXITS = schema([
    ("tradeId", I), ("decisionIndex", I), ("decisionMs", I), ("effectiveMs", I),
    ("known", B), ("event", B), ("status", S)])
PATH = schema([
    ("barIndex", I), ("barOpenMs", I), ("closePrice", F), ("balance", F), ("equity", F),
    ("side", I), ("quantity", F), ("segment", I), ("segmentEnd", B), ("windowEnd", B)],
    [("entryPrice", F), ("activeStopAtOpen", F), ("stopNextBar", F)])


def table(rows, schema):
    return pa.Table.from_pylist(rows, schema=schema)
