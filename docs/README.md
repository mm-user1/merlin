# Merlin Documentation Index

This is the complete entry point for tracked Merlin documentation. Use the
task routes below instead of treating every document as an equal authority.

## Source-of-truth hierarchy

1. An approved task specification governs its change.
2. Current executable behavior is represented by source, configs, schemas,
   and tests.
3. Exact certified values and external-parity evidence live in tracked
   baselines and the certification registry.
4. Stable current contracts live in the architecture and metrics references.
5. Procedures live in the relevant strategy, tool, Strategy Lab, or test guide.
6. Performance records and the changelog are historical evidence.

`docs/_work/` is ignored local planning/review history. It may be absent from a
fresh clone and is never required to understand, operate, or modify the
tracked repository.

## Start here

| Document | Audience and authority |
| --- | --- |
| [Root README](../README.md) | Users: product capabilities, installation, startup, and main UI workflow |
| [Repository guidance](../CLAUDE.md) | Coding agents: mandatory universal rules and task routing |
| [Agent bootstrap](../AGENTS.md) | Minimal tool-neutral pointer to repository guidance |
| [Project overview](PROJECT_OVERVIEW.md) | Current components, ownership, data flow, persistence/UI summary, and the sole complete strategy matrix |
| [Metrics](METRICS.md) | Complete cross-engine metric behavior and availability |
| [V1 optimizers](OPTIMIZERS.md) | Current V1 Optuna and strategy-owned Fast Grid contracts |

## Strategy work

| Document | Authority |
| --- | --- |
| [Legacy V1 strategy guide](ADDING_NEW_STRATEGY.md) | Maintaining or importing Backtester V1 strategies and optional V1 Fast Grid backends |
| [V2 strategy import guide](ADDING_NEW_STRATEGY_V2.md) | Primary procedure for new Grid-only Backtester V2 strategies |
| [Tracked S03 v10 Pine reference](S_03-Reversal_v10_for-import.pine) | Source provenance/example used by the legacy V1 import guide |

## V2 references

| Document | Kind |
| --- | --- |
| [V2 architecture](engine_v2/ARCHITECTURE.md) | Stable current execution, profile, Grid, runtime, WFA, and persistence contracts |
| [V2 certification](engine_v2/CERTIFICATION.md) | Evidence: certified profiles, external parity, tolerances, hashes, and preservation results |
| [V2 performance](engine_v2/PERFORMANCE.md) | Historical benchmark protocols, measurements, and conclusions |

## Tools and verification

| Document | Authority |
| --- | --- |
| [Tool catalog](../tools/README.md) | Maintained tool entry points and outputs |
| [Strategy Lab](../tools/strategy_lab/README.md) | V2-only research workflows, schemas, safety, generation, certification, analysis, and allocation |
| [Pattern Lab data foundation, studies, comparisons and bracket probes](../tools/pattern_lab/README.md) | Research-only pack, collector, evidence, identity, extensions and workers; M3a inference; M3b context and frozen validation; M4 sequential ATR bracket accounts, quantity rules, checked saved tables and offline descriptive reports (accepted at `c26334e` for the documented scope) |
| [Test guide](../tests/README.md) | Test tiers, isolation rules, and suite selection |

Pattern Lab M5 integrated research workflow is accepted at `d423016` within its
documented scope and the identity-portability limits of that generation; resume remains deferred.
Prospective policy-2 identities and verified extension relocation are accepted at
`9db9402` for the guide's documented scope, without changing historical M5 artifacts.
M4 remains accepted at `c26334e` for its documented scope.

## Baseline evidence

- [S01 V1 regression baseline](../data/baseline/README.md)
- [S03 Regime-ER V2 baseline](../data/baseline_v2/s03_reversal_v11_regime_er/README.md)
- [S03 v16-4-A Adaptive MA V2 baseline](../data/baseline_v2/s03_reversal_v16_4_a_adaptive_ma/README.md)
- [S06 R-Trend v02 V2 baseline](../data/baseline_v2/s06_r_trend_v02/README.md)
- [S06 Regime-TL V2 baseline](../data/baseline_v2/s06_r_trend_v02_regime_trendlines/README.md)
- [S06 v06-4-A2 V2 baseline index](../data/baseline_v2/s06_r_trend_v06_4_a2/README.md), which links its six reference-specific READMEs.

Baseline documents own their local provenance, parameters, expected values,
and interpretation. Do not modernize historical evidence to match newer
terminology.

## History

[changelog.md](../changelog.md) is the preserved release history. Certification
and performance documents may contain phase or task chronology because they
are evidence records; current architecture and procedure documents do not.

## Task-based reading routes

- Application, UI, routes, Queue, storage, or export: read the
  [project overview](PROJECT_OVERVIEW.md), then inspect the named source module.
- WFA windowing or WFE semantics: read
  [WFA and analytics](PROJECT_OVERVIEW.md#wfa-and-analytics); for V2 runtime
  rebasing, worker transport, delayed OOS, or plan reuse, continue to
  [V2 architecture](engine_v2/ARCHITECTURE.md#wfa).
- Metric formulas or availability: read [Metrics](METRICS.md).
- New strategy: use the [V2 import guide](ADDING_NEW_STRATEGY_V2.md). Use the
  [V1 guide](ADDING_NEW_STRATEGY.md) only for legacy V1 work.
- V1 Optuna or V1 Fast Grid: read [V1 optimizers](OPTIMIZERS.md), then the
  relevant strategy guide when package behavior is involved.
- Strategy-specific preservation: read the relevant V1/V2 strategy guide,
  then its certification or baseline evidence.
- V2 core or Grid change: read [V2 architecture](engine_v2/ARCHITECTURE.md),
  then certification and performance evidence if parity or speed is affected.
- Strategy Lab work: read its [full guide](../tools/strategy_lab/README.md).
- Pattern Lab data, collector, update, event-study, matched-comparison or report
  work: read its [guide](../tools/pattern_lab/README.md).
- Test planning: read the [test guide](../tests/README.md).
