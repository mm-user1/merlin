# Ticker selection and carried portfolios

Enable **Ticker Selector** in Start → Post Process to run one portfolio over the
selected Parquet pool. Capability is derived from the validated V2 profile across
all variants: declared Initial Capital, strict-close boundaries, no price rounding,
and either `risk_bracket_v1` or supported risk/fixed-percent sizing with a declared
fixed execution Contract Size. S07 and the five S03/S06 V2 configurations qualify. Initial Capital
is the total portfolio capital. Fixed works without WFA; Fixed/Grid also support
Calendar Months WFA. CSV, V1, day/Adaptive WFA, Grid without WFA, standalone OOS
holdout and DSR are unsupported combinations. Fixed retains its FT/Stress prohibition.

Choose one trading timeframe for the entire pool. Duplicate identical sources count
once; conflicting locators for one full instrument identity fail. Top N cannot exceed
the declared pool size. Entry Filters use their independently selected sources.
Start must be day 1 at midnight UTC. Only complete calendar windows are emitted from
the inclusive End; no incomplete trailing month is traded. WFA requires at least two
global OOS windows and rebalance cadence divisible by OOS months. Each required
IS/OOS/rebalance boundary must align with the epoch trading grid.

## Selector and data admission

The version-1 `tickerSelector` envelope saves `enabled`, `method`, `methodVersion`,
`topN`, `rebalanceMonths`, `evaluateUnselected`, and method `params`. The only method
is `meanBarRangePct` v1, displayed as **X — Mean Candle Range**. Defaults are Top 8,
one-month rebalance, comparisons Off, four-month lookback and 30-minute ranking bars.
Positive whole counts reject booleans and fractions. Ranking timeframe accepts m/h/D
and is a positive multiple of five minutes. Unknown active fields/versions fail;
disabled editor state is retained inert. Parameter Space Preview remains per-IS
Grid size (one configuration in Fixed), not a coverage certification or portfolio job count.

At each cutoff, X is `fsum(100 * (high-low) / close) / number_of_bars` over ordinary
complete bars in preceding complete lookback months. Scores sort descending, with
ASCII full-instrument identity breaking ties. Each month must independently have
at least 99% of its expected complete ranking bars, using integer counts. Bars that
straddle any month boundary count in neither month; zero-expected-bar months fail.
Multi-day/custom timeframes are checked against the actual calendar, not a day-divisor
allowlist. A verified listing after the lookback begins makes that ticker ineligible.
Missing older history without listing evidence is a preparation error, not an exclusion.
No future returns, HA candles or optimization results enter X.

All ranking and funded execution/filter slices are admitted before numerical work.
Funded slices include their own warmup and assigned IS/FT/OOS history; unused execution
history is not required. Existing strict target continuity and filter policies remain.
Locks are released before execution. Job-local monthly reuse avoids repeated reads.
The live-instrument pool cannot represent delisted instruments and may have survivorship bias.

## Funding and saved results

Legacy S03/S06 portfolio executions use `instrument_rules_v1` quantity policy.
Validated Parquet rules supply the effective base-unit step, minimum lots and
minimum notional. The existing IEEE sizing expression floors once; minimum checks
can refuse a new entry, including at its actual next-open price. They cannot prevent
a close or add a leverage cap, filter, stop or new rejection counter. Ordinary
CSV/Parquet executions retain their manual Contract Size; S07 retains risk admission.

Start disables Contract Size while this selector capability is active and displays
**From PQ instrument rules** (full wording in its tooltip). Its manual value remains in the editor/Queue;
turning the selector Off restores editing. Executed children instead store the
declared default as an inert parameter; the separate rules alone govern quantities.
Preview remains parameter space and reads no quantity-rule metadata or candles.
Results Parameters displays the child's saved effective rules; leverage diagnostics
remain unavailable, including flat and refused windows.

Each quantity-only child saves `quantity_execution={version:1,
mode:"instrument_rules_v1",rules:...}`. It contains the validated rule snapshot,
independently of `entry_admission`; transient rule objects and risk/filter/cap fields
are not serialized. Exact replay requires matching `portfolio_accounts` ownership
and saved identity for both Fixed and Grid/WFA children. Missing/invalid context,
changed consumed rule semantics, or an envelope on an unowned study fails clearly.
Manual tests must originate from a validated owned quantity child and verify its
source semantics before preparing their own interval. Saved tables remain readable
offline; relocation preserves original pins. No historical rows are migrated.

Every period allocates actual total / Top N to each place; missing eligible tickers
leave genuine idle cash. WFA selects at constant reference IS capital = original
total / Top N, then executes OOS at actual carried account capital. It does not scale
saved returns. Quantity rounding, admission and fees therefore use actual capital.
Strict close ends each window and period flat; even retained tickers require a new
eligible signal after rebalance. Nonpositive accounts stop executing until a later
positive pooled allocation; nonpositive total stops funding. Nonfinite arithmetic fails.

Evaluate Unselected runs independent hypothetical account-periods with the same
opening allocation. Their results never fund the parent. Optional failures retain
reason/status; required failures and cancellation publish nothing. The server creates
the mandatory PF set, real owned child studies and facts in one final transaction.
Browser completion and Auto-create Set do not own publication. Repeating a run creates
a distinct portfolio. Owned membership cannot be edited, and children cannot be
deleted alone; deleting the whole portfolio set uses existing confirmation behavior.

Analytics expands periods inline beneath PF sets. Checks aggregate; Alt focuses;
Shift/Ctrl, arrows and Esc follow the existing handlers. Parent checks cover periods
without double counting and become partial when appropriate. Parent focus shows ranks;
period focus shows tickers/idle places. Show Unselected sits before Group Dates and
appears only for periods with saved comparison entries from Evaluate Unselected runs.
Turning it off hides and unchecks comparisons.
Unavailable rows remain focusable but cannot be checked. Leaving portfolio scope restores
the outer Summary checks. Ordinary focused-set behavior and All Studies remain unchanged.

During scope or projection loading, the previous chart/cards remain dimmed with
`Loading...` and `aria-busy`; they are not the newly selected result. Old Summary
rows and links are inert until current detail arrives, while Study Sets navigation
remains available. Temporary table height prevents a transient main-area scroll
clamp; it is released on completion/error, without restoring over a user's scroll.
Current detail refreshes the shared checked/shown/total counters; total includes
saved comparison entries even when hidden. Only current replies can finish loading.
Actual empty/unavailable/error results clear stale visuals, and failed detail can
be retried by revisiting its scope. Valid result N/A values retain their meaning.
On exit to an uncached ordinary study, its current cards and chart become ready
together, including current metrics when the curve is empty or unavailable. A
rejected database-switch request preserves the current portfolio scope and saved
outer checks; confirmed switching still invalidates old replies at summary reset.

Use ↗ on an executed ticker row to open Results, then **Back to portfolio**. WFA shows
selected IS and actual OOS parameters/capital; Fixed without WFA has one reference result.
Stored Fixed children with matching database ownership replay their finite positive
internal capital even below or above the strategy editor bounds. Fixed without WFA
uses the saved period allocation; WFA keeps reference capital for IS and the saved
window capital for OOS. All other scalar/profile/runtime validation still applies.
Ordinary Fixed studies and new user requests retain editor bounds; client-supplied
portfolio metadata cannot enable this stored-child treatment. Existing affected
children require neither a new run nor a data repair.
Manual tests save separately and default to the selected window/period's execution
capital. Stored curves and summaries display offline. Reconstruction, trade export and
manual-source preparation verify consumed execution/filter/rule pins; append-only data
outside those bounds is accepted. Ranking provenance is saved without requiring re-ranking
or unrelated ranking files when opening an account. See [metrics](METRICS.md) for subset math.

Stitched WFA trade export skips explicit `no_trade` windows for both ordinary and
portfolio studies, matching per-window export. It never reconstructs trades for a
window deliberately rejected by Forward Test. An all-no-trade export is the normal
header-only CSV. This does not change ordinary delayed-window scheduling.

## Implementation ownership

`core/ticker_selector.py` owns settings/calendar/scoring; `core/portfolio.py` coordinates
existing Grid and the extracted `WalkForwardEngine.run_window`. Generic reference/Numba
kernels remain authoritative. `core/portfolio_projection.py` owns offline arithmetic.
`core/portfolio_storage.py` adds `portfolios` (parent linked to one `study_sets` row),
`portfolio_accounts` (real child ownership), and the `ordinary_studies` view. Versioned
parent JSON contains periods, ranking provenance and account balance paths. WFA windows
add nullable `oos_execution_capital`; connection-aware saves retain ordinary behavior.
No historical result conversion or portfolio backfill runs. Ordinary enumerations exclude
children; explicit Results IDs remain valid. Name uniqueness checks still consider all
real study names. No MTM arrays/metrics or separate account ledger UI are added.

`POST /api/portfolio` accepts one complete executable pool request. Saved-view/projection
routes under `/api/portfolios` accept owned identities, deduplicate them and compute in
Python. Analytics extends its existing set/table handlers and chart renderer; browser
code owns selection and presentation only.
