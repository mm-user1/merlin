# Fixed entry filters

Entry filters independently allow or block raw long/short signals before Grid
evaluation, ranking and reference execution. They never generate signals, queue
a blocked signal, alter exits, resize positions or recheck at next-open fill.
Optimization result filters instead screen completed runs.

## Capability and editor

New V2 strategies explicitly declare top-level
`"entryFilterSupport":{"version":1,"methods":["candleDirection"]}`, outside
`execution` and `parameters`. Every variant must satisfy the existing
`risk_bracket_v1` strict-close position composition. No production strategy is
automatically opted in. V1, CSV and signal-reversal strategies cannot run active
filters. Auxiliary inputs, filter optimization, pyramiding and partial exits are
not implemented.

Start places Entry Filters after the compact strategy selector; selector metadata
remains in collapsed Details. Master/item Off retain settings. Add creates a
stable item ID and method v2 defaults (Confirm 2 / Break 1). Selecting a capable
strategy does not activate filters. Unsupported strategies disable editing but retain master Off;
active unsupported settings block launch. Queue preserves editor settings and
unknown inactive records, restoring authoritative `entryFilters`. Presets do not
gain entry-filter persistence.

The request/Queue namespace is separate from fixed strategy parameters:

```json
{"entryFilters":{"version":1,"enabled":true,"items":[{
  "id":"filter-1","type":"candleDirection","version":2,"enabled":true,
  "parameters":{"confirmationBars":2,"breakBars":1},
  "source":{"type":"parquet","version":1,"pack_root":"/absolute/pack",
    "instrument_id":"OKX_BTC-USDT-SWAP","timeframe_minutes":240}
}]}}
```

F06 · Candle Direction uses ordinary closed candles. Defaults are Confirm Bars 2,
timeframe 4h and exactly `OKX_BTC-USDT-SWAP`. A new filter uses the current Parquet
directory (the remembered Parquet directory in CSV mode), otherwise the server's
resolved `data/parquet-data` default. Its actual path is populated and metadata
loaded through normal allowed-root browsing. A missing pack or BTC instrument
shows an actionable inline message, retaining the chosen path and exact instrument;
there is no venue fallback or automatic substitution. Browse remains available.
Restored filters retain their source, and later target changes do not rewrite it.
Obsolete metadata replies after removal, path changes or restoration are ignored.
Other valid OHLCV instruments are
allowed without sizing rules; only the trading target needs those rules.
Timeframes follow the positive 5m-multiple m/h/D contract. Confirm Bars is an
integer, excluding booleans and fractions. Method v2 requires Confirm N=2..6,
Break M=1..4, and M<N. Historical method v1 retains its integer N>=2 domain and
has no Break parameter. Editing a v1 count, source or toggle preserves v1; only
an explicit valid Break edit converts it to v2. For a saved v1 N>6, change N to
a valid v2 value before conversion. Master/item Off and Queue retain either
version. Multiple active filters intersect per side; v1 and v2 identities differ.

Invalid active instrument IDs use the provider's validation rules and are reported
as application input errors, including HTTP 400 at launch. An invalid ID in saved
filter settings makes that study's offline description Unavailable; other Analytics
studies remain visible. Reads do not repair saved settings. Inactive descriptor
content remains unused while master or item Off applies.

## Native clock and source bounds

Colour is +1 for close>open, -1 for close<open and zero for neutral. In v1, consecutive
colour count resets on neutrality, reversal or gap. With N confirmation bars,
P=max(28,N-1) prior contiguous native candles are also required. Zero-based segment
position >=P is known; the default first known candle is the 29th. Positive or
negative runs >=N permit long or short. Known neutral/unconfirmed states block both.

V2 tracks consecutive green/red counts and a directional state. N green/red
candles activates long/short. M opposite candles blocks the active direction;
fewer retain it. Neutral resets both counts and direction. Gaps reset direction,
counts and the 29-contiguous-candle readiness clock (P=28). Thus v2 M=1 matches
v1 permissions for the same N, while M>1 can retain state indefinitely through
alternating candles. Unknown states block both directions.

A source close at T publishes at T and expires at T+S exclusively, where S is its
declared duration. Target decisions use open plus declared target duration. The
newest publication <=decision wins, including coincident closes. New unknown or
neutral states replace older directional states; missing publications expire.

From admitted target evaluation bounds, excluding target warmup, resolve:

```text
D0 = bounds.start + targetDuration
D1 = bounds.end
boundedStart = floor(D0 / S) * S - (P + 1) * S
sourceEnd   = floor(D1 / S) * S
```

V1 and v2 M=1 use sourceStart=boundedStart. V2 M>1 reads once from the aligned
declared source origin: `ceil(first_open_utc / S) * S`, which must be representable
and no later than boundedStart. Metadata and candles use the same read session.
There is no backward anchor search. Gaps still reset state and readiness.

Read `[sourceStart, sourceEnd)` on the UTC epoch grid. Integer bounds are checked
before timestamp construction. Changing target Warmup does not change filter
history for an unchanged evaluation interval. Date Filter Off never shifts Start
forward to accommodate filter history.

Coverage/closure shortage, bad units/schema/state, pending updates, corruption,
duplicates/off-grid observations and an entirely empty complete-source slice
refuse execution. A short declared prefix names the required start and suggests
a later Start. Gaps inside admitted coverage, including the initial prefix and
first decision candle, block until native warmup recovers. A nonempty source that
never becomes known is valid and blocks throughout. Target continuity stays strict;
filter reads use the public gap-aware slice API.

`core.entry_filter_input` groups reads by resolved root. Target and filters sharing
a root use one session; independent roots are serial, without nested ownership or
a cross-pack atomicity claim. Prepare once per required input/configuration, then
align by timestamp across windows. Workers receive RAM frames, rules and permissions.
Preview/discovery performs no candle reads or hashes. The accepted non-ready-state
Preview limitation also applies here: a schema-valid incomplete pack may preview,
but actual loading refuses it.

## Execution and identity

`core.entry_filters` applies `rawLong & permitLong` and `rawShort & permitShort`.
Existing long-first selection among enabled remaining signals is unchanged. A raw
dual bar can trade short when long is blocked and the existing short is permitted.
All-permissive equals Off for both side-enable settings. Raw cached arrays, exits,
dataprep and the low-level kernel ABI remain unchanged.

Capable hooks/direct runners require explicit `OFF` or `PreparedPermissions`;
`MISSING` is an error even when intended settings are Off. Only launch normalization
may create Off from absent/default editor settings. Hook wrappers cover single and
batch builders. Grid Fast/selected, FT/Stress workers, OOS, fixed/adaptive WFA,
manual and reconstruction transport context beside frames and rules. Permission
origins never reset at window starts; execution validates timestamp alignment.

Active Grid identity includes normalized effective descriptors. It excludes IDs,
order, identical duplicate AND items, disabled rows, consumed hashes and dates.
Off adds no identity key. Original locators participate, with Windows case and
separator normalization even on Linux without filesystem access. Verified overrides
do not change original configuration. WFA plan reuse distinguishes effective
settings; each execution still supplies the correct aligned context.

## Persistence, replay and display

New capable executions save authoritative `entryFilters` and versioned
`entry_filter_inputs`. Explicit Off is `{"version":1,"state":"off"}`. Filtered
facts contain effective descriptors, method policy, native bounds/origin,
priorBars, instrument facts and bounded fingerprints including absent observations.
Prepared arrays never enter saved configuration or Queue.

V1 retains policy `closed_candle_direction_v1` and its existing pin shape. V2 uses
`closed_candle_direction_v2`, with `originRule="bounded_prefix"` for M=1 and
`originRule="declared_source_origin"` for M>1. Saved bounds include start, end,
origin=start and priorBars=28. Offline validation checks version, N/M, policy,
origin rule, native alignment and the relationship to target bounds. V2 display
requires the target pin and shows the version plus N / break M.

Backtest returns `entry_filter_inputs`; download must post it as `entryFilterInputs`
with `entryFilters` and existing target/admission facts. Missing, null, unsupported
or conflicting facts refuse replay. Non-capable strategies refuse injected inputs.
Enabled settings cannot use Off facts. Genuine pre-feature tables stay readable;
current capable execution cannot invent missing context.

Changed consumed candles/gaps/settings/policy refuse exact replay. Unused history
extensions and physical rewrites preserving consumed facts remain compatible.
Original-source manual tests verify parent target/filter pins, then prepare and
save their new interval. For v2 M>1, replay, relocation and parent-based manual
execution reuse the saved source start, even if current metadata has gained
earlier history. All history consumed since that start remains fingerprinted;
an early repair refuses replay even if eventual permissions happen to agree.
Extensions outside consumed bounds remain compatible. Locate Parquet Pack selects target or filter dependency.
All filter bindings sharing a selected original locator are verified before saving
an override. Original pins stay unchanged; target relocation is separate.

Results Parameters describes the selected saved execution, including manual
context, or Filter Off. Analytics Research Info adds one dynamic Entry Filters row;
other rows remain database-wide. Individual focus takes priority, then actual
set-focus/checked-set membership, then the filtered All Studies context. Orphan IDs
and overlays do not affect membership. All Studies ignores retained checked sets.
Empty is `?`; any malformed new context dominates as Unavailable. Equal effective
settings show their description, all Off shows Filter Off, otherwise Mixed. Display
never reads packs.

See [Parquet inputs](PARQUET_INPUTS.md), [V2 architecture](engine_v2/ARCHITECTURE.md)
and [verification guidance](../tests/README.md).
