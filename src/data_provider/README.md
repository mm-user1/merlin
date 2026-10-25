# Shared data provider

Merlin's [Parquet integration](../../docs/PARQUET_INPUTS.md) consumes this API with
strict target continuity in `core.data_input`, explicit prepared-frame transport
and application replay pins. That integration does not change shared reader bytes,
pack schemas, fingerprints, collector behavior or research attribution generations.
Keep separate physical packs for independent Merlin and Pattern Lab operation.

This package owns Parquet market-data packs: validation, bounded reads,
resampling and fingerprints (`data`, `manifest`), exclusive process locks
(`pack_lock`), recoverable publication (`update_transaction`), public exchange
adapters (`exchange_data`) and explicit collection (`collect`). It imports no
Pattern Lab, Flask, strategy or Merlin execution code. PyArrow remains lazy;
network requests occur only in explicitly invoked acquisition operations.

Put this checkout's `src` directory on the Python import path and import
`data_provider`, never `src.data_provider`. Pattern Lab supplies its own small
bootstrap and exposes these same six module objects as package attributes.

```python
from data_provider import data, collect

with data.read_session(pack_root) as session:
    loaded = session.load_slice(instrument_id, start=start, end=end,
                                warmup_start=warmup, timeframe_minutes=30)
collect.collect_pack(new_root, start=start, end=end, roster_path=roster_path)
```

Roots and roster inputs are caller-owned. There is no implicit default roster:
missing roster path/configuration raises `invalid_roster`. Pattern Lab's tracked
roster remains available as `tools.pattern_lab.DEFAULT_ROSTER_PATH`; its CLI
requires `--universe`. Progress is an optional caller callback. Acquisition
records `source.collector="data_provider.collect"` prospectively.

Public operations retain their existing signatures except explicit roster
selection: `publish_pack`, `inspect_pack`, `load_slice`, `read_session`,
`collect_pack`, `update_pack`, `recover_pack`, `abort_update`, and the concrete
validation/serialization helpers. The NPZ adapter stays in Pattern Lab.

`errors` defines `DataProviderError`, `DataError`, `DependencyError`, `BusyError`
and `PendingError`. Stable machine codes, messages and per-instance `error_code`
overrides remain compatible. The base default is historically
`pattern_lab_error`. Pattern Lab's existing exception names alias these types;
its contextual study error remains research-owned.

The [Pattern Lab pack contract](../../tools/pattern_lab/README.md#pack-layout)
remains authoritative for schemas, validation, bounded intervals, complete UTC
resampling, no-fill gaps, exact input fingerprints, collector provenance and
staged recovery. No storage or numerical policy changed in A1. Exclusive,
nonblocking `.pack-lock` guards remain per resolved root; even readers require
lock-file access. Use separate real roots for independent tools. No automatic
data synchronization, shared-reader locks or strict target continuity policy
is introduced here.

`READER_MODULES` declares the seven-source reader closure for research
attribution. Runtime hashes use exact source bytes; scoped Git attributes retain
LF. Historical artifacts keep their saved identities. Candidate-generation and
offline compatibility rules belong to the
[research guide](../../tools/pattern_lab/README.md#shared-data-provider-a1).
