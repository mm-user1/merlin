# Comparative report schema

Authority for newly published field-audit v2, screen v1 and Fixed-comparison v2
reports. See [usage and interpretation](README.md#saved-result-audits-and-comparative-screens),
[nested analysis](README.md#analyze-development-data) and
[nested allocation](README.md#allocate-fixed-capacity). Nested contracts are unchanged.
The thirteen distinct CSV filenames below include three report-specific
`concentration.csv` contracts (fifteen table contracts in total).

## Encoding, common columns and units

UTF-8 CSV has a header, LF row terminators and ordinary CSV quoting. Structured
cells are compact JSON with sorted object keys. JSON lists retain their stated
order. CSV null is an empty cell; booleans are `True`, `False`, or empty for null.
JSON uses `true`, `false`, `null`. Numeric null never means zero. Quantiles use
linear interpolation on finite values. Nonfinite values do not become samples.
Empty tables have the sole header `status`; a populated union-of-levels header
contains all applicable columns, with inapplicable fields empty. Column groups
below are exact field sets; `+` means their union, not a literal column. Consumers
should address columns by name, not position.

Common **S** (finite statistics): `expected`, `finite`, `missing`, `positive`,
`positive_share_expected`, `positive_share_finite`, `mean`, `median`, `q25`,
`q75`, `minimum`, `maximum`, `reason`. Positive means strictly greater than zero;
shares are fractions, not percentages. Expected includes unavailable observations.
An empty finite sample has positive count zero, null finite share/numerical
statistics and `no_finite_values`; expected share is zero when expected is nonzero.

Common **R** (hierarchical field reduction): `mean`, `minimum`, `maximum`,
`expected_cells`, `finite_cells`, `missing_cells`, `finite_tickers`, `finite_blocks`,
`positive_cells`, `positive_share_expected`, `positive_share_finite`, `reason`.
Mean is equal finite tickers per block, then equal available blocks; extrema and
positive counts describe underlying cells. Empty support gives `no_finite_values`.

Common **N** (neighborhood): `expected_members`, `finite_members`,
`finite_distinct_neighbor_count`, `status`, `partial_support`, `center`, `mean`,
`worst`, `positive_share`, `center_minus_mean`. Center belongs to the support.
`insufficient_neighbors` means no distinct declared neighbor;
`no_finite_distinct_neighbor` means declared neighbors exist but none is finite;
both suppress mean/worst/share/delta. Otherwise status is `available`. Nonfinite
center suppresses only its relative delta. Positive share is a finite-member fraction.

Common **C** (additive concentration): `group`, `contribution`, `expected`,
`finite`, `observed_subtotal`, `complete_total`, `partial_support`, `positive_gross`,
`absolute_negative_gross`, `positive_gross_share`, `largest_positive_group`,
`largest_positive_share`, `total_excluding_largest`, `reason`. Expected/finite
count constituent contributions to this group. Gross/share and totals describe
all groups in the containing candidate/variant/channel context, not just this row.
Positive shares divide by positive gross; negative gross is an absolute magnitude.
No positive gross gives null shares/removal and `no_positive_contribution`;
incomplete totals give `incomplete_contributions` (takes precedence). Removal is
subtraction without reranking or reweighting. Largest-positive references resolve
to another group in the same context. Default ties and group rows sort by string
group ID; field time groups use the OOS-start ordering described below.

Metrics are `net_profit_pct`, `max_drawdown_pct`, `max_drawdown_mtm_pct` (percent)
and `total_trades` (count), unless explicitly stated otherwise. Differences of
percent metrics are percentage points; trade differences are counts. Fractions,
Spearman and support counts are dimensionless. Currency values retain the saved
account currency. Realized balance drawdown and bar-close MTM drawdown are distinct.

## Field audit: audit.json, strategy_lab_field_audit_v2

Candidate fields **K**: `semantic_key`, `candidate_id`, `params`, `segment`.
`params` is a scalar-parameter JSON object, ID is local to the saved input,
semantic key joins its candidate geometry, and segment is `is` or `oos`.

### cell_summary.csv

Exact columns: `ticker`, `window_id`, `segment`, `metric` + **S** +
`zero_trade_count`, `spearman`, `paired_finite`, `spearman_reason`, `feasibility`,
`selected_rule`, `selected_semantic_keys`, `selected_oos`,
`selected_minus_population_mean`, `selected_minus_population_median`.

One ticker/window/segment/metric row reduces the candidate population. Zero-trade
count uses that segment's trade metric, independently of the reduced metric.
Spearman compares paired IS/OOS values; unavailable reasons are
`fewer_than_eight_pairs` or `constant_vector`. `selected_semantic_keys` is a JSON
list of at most one frozen IS key. Selected OOS and its deltas always describe
net-return OOS, repeated across the cell's metric/segment rows. `feasibility`
describes IS policy eligibility and is a JSON object: either `status=not_requested`,
or `status=requested`, `finite_objective_count`,
`constraint_pass_counts` (policy-order list), `eligible_count`, `no_eligible`.

### candidate_summary.csv

Exact columns: **K** + `metric`, `level`, `ticker`, `window_id` + **R**.
`level=candidate` has all R fields over ticker/window support and null ticker/ID.
`level=ticker` (net return only) identifies ticker and reduces its windows;
`level=block` (net return only) identifies window and reduces its tickers.
Both narrower levels populate only `mean`, `minimum`, `maximum`, `expected_cells`,
`finite_cells`, `missing_cells`, `positive_cells`, `reason` from R. Other R fields
and the unused ticker/window field are empty.

### axis_marginals.csv

Exact columns: `segment`, `metric`, `geometry_block`, `active_mask`, `axis`,
`code`, `value`, `candidate_count`, `expected_candidate_cells`,
`finite_candidate_cells` + **R**. Geometry block and active mask are JSON arrays;
code is the saved generated axis code, value its scalar setting. Within each
block/mask/active-axis value, finite candidates first reduce to one ticker/window
value, then R applies. Candidate-cell counts describe the unreduced population.

### neighbour_stability.csv

Exact columns: `level`, `semantic_key`, `candidate_id`, `segment`, `ticker`,
`window_id` + **N** + `expected_cells`, `eligible_cells`, `insufficient_neighbors`,
`no_finite_distinct_neighbor`, `incomplete_cells`.

`level=selected_cell` identifies the frozen IS-selected candidate/ticker/window;
N is its exact local support and the five cell-aggregate fields are null.
`level=candidate` has null ticker/window; expected members is geometric size,
finite members/distinct count are null. Mean, positive share and center-relative
delta reduce eligible cell statistics hierarchically; worst is their minimum.
Center is the candidate's hierarchical net return; status is `available` if any
cell is eligible, otherwise `unavailable`. The five cell fields count all
ticker/windows, eligible cells, each unavailable status and incomplete support.

### concentration.csv (field)

Exact columns: **K** + `dimension`, `unit` + **C**. Dimension is `ticker` or
`time`; unit is `percentage_points_of_hierarchical_mean`. Ticker contributions
retain the within-block finite-ticker weights, divided by available block count;
time contributions are block means divided by that count. Both reconcile to
the candidate hierarchical net-return mean. Partial support discloses missing
cells; the total still describes the observed finite-support mean.

Time group is `window:<integer window_id>`. `audit.json.window_groups[group]`
contains `window_id`, `is_interval=[start,end]`, `oos_interval=[start,end]`,
preserving saved endpoint strings. Segment selects the interval. The mapping
also resolves `largest_positive_group`. Overlapping IS windows remain separate
descriptive blocks. Time rows and equal-largest ties sort by saved OOS-start
string, then integer window ID for identical starts; key formatting never
changes the historical OOS-start tie order. Ticker groups retain string order.

## Screen: screen.json, strategy_lab_screen_v1

### comparisons.csv

Exact columns: `level`, `baseline`, `dataset_label`, `variant`, `block_key`,
`baseline_value`, `input_value`, `delta`, `unit`, `reason`,
`baseline_selected_tickers`, `input_selected_tickers`, `baseline_unit`,
`input_unit`, `delta_unit`, `baseline_selected_count`, `input_selected_count`,
`expected_blocks`, `finite_paired_blocks`.

Only requested baseline comparisons produce rows. Block key is JSON
`[oos_start,oos_end]`; selected tickers are JSON lists. Values are percent,
delta is input minus baseline in percentage points. Variants are the existing
allocation variants plus `all_available`. Block rows have null finite-paired
block count. Headline rows have null block key, ticker lists/counts; they show
equal-block means only when every expected paired block is finite, otherwise
`incomplete_paired_blocks`. Block reasons: `unavailable_capacity_block`,
`no_finite_tickers`, `different_finite_ticker_sets`; successful rows have null
reason. All-available requires identical nonempty finite ticker sets.

### candidate_turnover.csv

Exact columns: `dataset_label`, `ticker`, `changed`, `unchanged`, `unavailable`,
`valid_transitions`, `changed_share`. One dataset/ticker row compares frozen keys
in adjacent common blocks, including gaps. Either missing key makes the
transition unavailable. Valid transitions = changed + unchanged; share is
changed/valid, null if no valid transition. OOS and common-pool status do not gate it.

### concentration.csv (screen)

Exact columns: `dataset_label`, `variant`, `dimension`, `unit`, `expected_blocks`
+ **C**. Dimension is ticker or time (OOS-start string); `cash` denotes a block
with no allocated members. Unit is `percentage_points_of_mean_capacity_return`.
Each saved capacity contribution divides by expected common-block count.
Unavailable blocks suppress their member contributions and the complete total;
observed subtotal remains descriptive. Cash contributes zero.

### filter_candidate_pairs.csv (strict filter pairs only)

Exact columns: `ticker`, `block_key`, `segment`, `semantic_key`, `params`,
`interpretation`, `off_manifest_sha256`, `off_candidate_id`,
`off_net_profit_pct`, `off_max_drawdown_pct`, `off_max_drawdown_mtm_pct`, `off_total_trades`,
`on_manifest_sha256`, `on_candidate_id`, `on_net_profit_pct`, `on_max_drawdown_pct`,
`on_max_drawdown_mtm_pct`, `on_total_trades`,
`delta_net_profit_pct`, `reason_net_profit_pct`, `delta_max_drawdown_pct`,
`reason_max_drawdown_pct`, `delta_max_drawdown_mtm_pct`, `reason_max_drawdown_mtm_pct`,
`delta_total_trades`, `reason_total_trades`.

One semantic candidate/ticker/block/segment row. Params is a JSON object;
block key a JSON endpoint pair. Interpretation is `same configuration`. Each
delta is On minus Off; missing either finite metric gives `nonfinite_pair`.
Local candidate IDs need not be equal; manifests resolve to descriptor sources.

### filter_selection_pairs.csv (strict filter pairs only)

Exact columns: `ticker`, `block_key`, `interpretation`,
`off_semantic_key`, `off_manifest_sha256`, `off_is_net_profit_pct`,
`off_selection_available`, `off_ticker_score_available`, `off_in_common_pool`,
`off_net_profit_pct`, `off_max_drawdown_pct`, `off_max_drawdown_mtm_pct`, `off_total_trades`,
`on_semantic_key`, `on_manifest_sha256`, `on_is_net_profit_pct`,
`on_selection_available`, `on_ticker_score_available`, `on_in_common_pool`,
`on_net_profit_pct`, `on_max_drawdown_pct`, `on_max_drawdown_mtm_pct`, `on_total_trades`,
`delta_net_profit_pct`, `reason_net_profit_pct`, `delta_max_drawdown_pct`,
`reason_max_drawdown_pct`, `delta_max_drawdown_mtm_pct`, `reason_max_drawdown_mtm_pct`,
`delta_total_trades`, `reason_total_trades`, `same_candidate`.

One ticker/block row with interpretation `policy reselection with filters`.
Keys are independently frozen IS choices; unprefixed segment is implicitly OOS.
Selection availability means key exists; score availability means finite local
selected-IS net return; pool membership means finite common-pool ticker score.
Metrics are each local selection's saved OOS even when excluded from the pool.
Missing selections/nonfinite OOS give `missing_selection_or_nonfinite_oos` for
the affected delta. Additive v1 `same_candidate` compares semantic keys if both
exist, otherwise null; CSV encodes `True`/`False`/empty. IDs, scores, pool and OOS
availability never gate this flag. Allocation estimands remain in comparisons.

## Fixed comparison: comparison.json, strategy_lab_fixed_comparison_v2

### accounts.csv

Exact columns: `dataset_label`, `schema_version`, `manifest_sha256`, `account_id`,
`parameter_set`, `period_name`, `ticker`, `cell`, `timeframe_minutes`, `period_key`,
`period_kind`, `variant_key`, `variant`, `stratum_key`, `stratum`, `filter_key`,
`strategy_params`, `params`, `economics`, `status`, `reason`, `input_pin`,
`admission`, `provenance`, `observation_count`, `metrics`, `metric_units`,
`metric_unavailable_reasons`.

One declared account, including failed/pending. Account ID is globally unique.
Period kind is `independent_run`; period key is exact UTC evaluation endpoint
JSON pair. Variant, stratum, strategy_params, params, economics, input_pin,
admission, provenance, metrics, metric_units and metric_unavailable_reasons are
JSON objects. Variant identifies producing strategy/scalars/filters/TF; stratum
identifies strategy/execution/economics/funding/slippage. Descriptor dictionaries
join variant/stratum keys. Manifest hash joins source label; parameter/period
names are display facts. Input pin retains fingerprints, instrument and all
saved bounds; admission preserves operative rules. Status is completed/failed/
pending, with null reason, saved failure reason or `not_completed`. Completed
metrics include the saved result's full metric dictionary, units and individual
unavailability reasons; incomplete accounts have empty dictionaries/null count.

### calendar.csv

Exact columns: `dataset_label`, `account_id`, `variant_key`, `stratum_key`,
`ticker`, `period_key`, `period_kind`, `month`, `opening_equity`, `closing_equity`,
`equity_return_pct`, `return_unavailable_reason`, `opening_balance`,
`closing_balance`, `realized_pnl`, `equity_change`, `first_timestamp_ns`,
`last_timestamp_ns`, `observation_count`, `partial_month`.

One completed account/month (`YYYY-MM`); period kind `continuous_calendar_slice`.
Join account ID to accounts. Equity/balance/P&L/change are currency; return is
percent using saved opening equity, unavailable for a nonpositive opening
denominator (`nonpositive_opening_equity`). Timestamps are UTC nanoseconds.
Partial month is boolean; no monthly drawdown or trade count is inferred.

### cohort_facts.json: strategy_lab_fixed_cohort_facts_v1

Authoritative shared JSON, listed and hashed in comparison.json and linked from
HTML. Root keys: `schema_version`, `cohorts` (object keyed by cohort ID). Each
context has `stratum_key`, `period_key`, `union_tickers`, `variants`, `metrics`.
Variants maps variant key to `{ticker: account_id}`. This declares roster D_v,
including incomplete accounts; account references supply status/metric reasons,
input fingerprint, exact bounds, timeframe and admission without duplicate payloads.
U is union_tickers. Metric records map each of the four metric names to
`finite_tickers` (variant -> sorted F_v), `common_tickers` (sorted C, intersection
of all F_v), `sample_classification`. F_v requires completed and finite metric.
Recover declared missingness as D_v minus F_v and cohort coverage exclusions as
U minus F_v. Common sample facts are account references restricted to C.

Cohort ID is the existing canonical SHA-256 digest of
`{stratum_key, period_key, variant_tickers}`, where variant_tickers maps sorted
variant keys to sorted declared ticker lists. It includes exact period, stratum
and membership, never source paths. Shared JSON uses canonical sorted object
keys; all roster arrays sort ascending. Metric join is `(cohort_id, metric)`.
Classification is `unavailable_common_membership` for empty C,
`identical_saved_sample_facts` for equal per-ticker fingerprint/bounds/TF/admission
across variants, else `descriptive_common_membership`. It describes the common
sample only. Source account IDs do not assert causal comparability.

### summaries.csv

Exact union header: `level`, `variant_key`, `stratum_key`, `period_key`, `metric`,
`membership`, `cohort_id`, `expected_count`, `cohort_expected_count`,
`declared_count`, `completed_count`, `no_trade_count`, `sample_classification`
+ **S** + `positive_share_expected_cohort`, `observed_sum`, `complete_total`,
`primary_values`, `overlapping_periods`, `timeframes_minutes`, `evaluation_geometry_varies`.

Primary rows use membership available/common and join the shared context/metric
and variant. Available expected roster is D_v, finite is F_v; common expected
roster is U, finite is C. Missing lists are the corresponding set difference;
no roster payload columns remain in CSV. `expected_count=expected` is the mode's
expected size; cohort_expected_count is |U|. Positive/expected uses this mode's
expected size; positive/expected-cohort always uses |U|. Declared/completed/no-trade
counts remain variant-roster facts; no-trade counts completed zero-trade accounts.
Observed sum applies only to total_trades (null for an empty finite sample);
complete total requires finite=expected. Empty C gives `empty_common_sample`,
zero positive/expected and null numerical statistics. Cross-only fields are null.

`level=cross_period` or `cross_timeframe` uses `variant_key`, `stratum_key`,
`metric`, `primary_values`, `finite`, `minimum`, `median`, `maximum`,
`overlapping_periods`, `timeframes_minutes`, `evaluation_geometry_varies`, `mean`,
`reason`; remaining columns are null. Primary_values is a JSON list of
`{variant_key, period_key, value}` available primary means. Cross-timeframe's
variant key hashes the variant with timeframe removed. Timeframes is a sorted
integer JSON list. Finite/count/extrema summarize these separate estimates;
mean is null, reason explains independent possibly overlapping runs and geometry.

### neighbours.csv

Requested-axis union header: `level`, `center_account_id`, `neighbor_account_id`,
`ticker`, `period_key`, `axis`, `distance`, `metric`, `center`, `neighbor`,
`difference`, `comparable`, `status`, `reason` + **N** (deduplicating center/status).
If no center has a declared full neighbouring configuration reachable by one
adjacent requested-axis step, no edge-only columns occur. This includes singleton
groups and disconnected sparse configurations. The exact edge-free header is
`level`, `center_account_id`, `ticker`, `period_key`, `metric` + **N**.
Without requested axes the header is `status`, `reason`, with `not_requested`
and the axis command hint.

Edges join account IDs; nullable neighbor ID means `undeclared_ticker_period_cell`.
For each center, requested axes retain caller order, lower neighbor then upper.
Domains come from declared configs within stratum/filter/TF. Change one axis by
one adjacent domain value and require that full configuration to exist; no
bridging a missing full config. Distance is signed axis-value difference. Each
edge has all four metrics; difference is neighbor minus center if both finite.
Comparable status requires equal target pin/admission/TF and exact ticker/period;
otherwise `different_target_pin_admission_or_evaluation_geometry` suppresses
neighbor values. Pending/nonfinite compatible cells retain null metrics without
changing declaration. Neighborhood rows use net-return **N**; edge-only fields
are null. Direct account keys reject ambiguous matches.

### concentration.csv (Fixed)

Exact columns: `variant_key`, `stratum_key`, `period_key`, `unit`, `currency`,
`group_expected_accounts`, `group_completed_accounts`, `interpretation`,
`account_id`, `dimension`, `channel` + **C**.
Unit is `saved_account_currency`; interpretation states independent-account
additive currency, no shared portfolio. Context also includes quote currency.
Ticker dimension uses realized_pnl and null account ID; time dimension groups
calendar months separately for realized_pnl/equity_change, both per account and
aggregate (null account ID). Account ID joins accounts/calendar; no percentages
are summed or independent periods chained. Complete totals require the group's
completed finite account P&L (per-account time rows require completion).

All descriptors preserve source identities and hash/size claims for every CSV,
shared JSON and HTML artifact. They publish last as completion markers. Fixed
explicit universe resolution requires nonempty common effective input roster,
while preserving saved per-input subsets/order. Default union is allowed;
empty metric-specific common samples remain explicit unavailable rows.
