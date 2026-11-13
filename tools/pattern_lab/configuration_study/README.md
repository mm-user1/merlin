# Compact configuration studies

Runs every declared configuration independently over IS, HO1 and HO2 through
native accounts. Retains exact metrics and display curves, not full execution
evidence. It cannot supply Universe Selection replay or native account report
regeneration/import. This backend generates no HTML. Full native studies remain
available. [Comparative reports](../configuration_report/README.md) rebuild
standalone owner/public HTML from compact results without another execution.

## API

```python
from tools.pattern_lab.configuration_study import (
    normalize_request, run_configuration_study, load_configuration_study,
    verify_configuration_study, export_public_results, normalize_annotations,
)
request = normalize_request("request.json")
owner = run_configuration_study(request, data_root="pack", output_root="owner",
                                workers=1, auxiliary_roots=None)
public = export_public_results(owner.root, output_root="public")
stored = load_configuration_study("public")
config = stored.configurations[0]
row = stored.result(config["configurationId"], "IS")
curve = stored.curve(config["configurationId"], "IS")["curve"]
price = stored.price(config["instrumentId"], "IS")["price"]
annotations = normalize_annotations("annotations.json", stored)
```

CLI: `python -B -m tools.pattern_lab configuration-study --request REQUEST
--data-root PACK --output-root OWNER --workers 1`; `configuration-verify
--run-root OWNER`; `configuration-public --run-root OWNER --output-root PUBLIC`.
Auxiliary requests use repeatable `--auxiliary-root ALIAS=PATH`.
`examples/configuration_synthetic.py` creates a trade-bearing synthetic example.

Normalization returns an immutable `ConfigurationStudyRequest` with a fresh
`document` property. Closed schema `pattern-lab.configuration-study-request.v1`
requires `schema`, `studyId`, `instruments`, `sources`, `allocations`, `stages`;
optional `notes`, `auxiliaryInputs`. Ordered IS/HO1/HO2 stages have `stageId`,
`startUtc`, `endUtc`, `preparationStartUtc`, `protocol` (embedded native document
or request-file-relative path). Each native protocol admits its own stage/warmup;
earlier reserves contain all later stages. Auxiliary-field presence selects native
v2 even when empty; otherwise v1. Native policy/provider/filter semantics apply.
All allocations share initial capital; equivalent numerical duplicates are refused.

Annotations are separate unsealed documents: schema
`pattern-lab.configuration-annotations.v1`, `specificationIdentity`, `annotations`.
Each entry has `instrumentId`, `allocationId`, `role` (`return`, `stability`,
`reference`), `methodId`, `methodDescription`, `selectedFromStage` (`IS` only).
Validation sorts instrument/allocation/role triples, rejects duplicates, and reads
metadata only. It works on owner and public without HO2 decoding or execution.

## Stored reader and identities

Fresh properties: `metadata`, `configurations`, `chunk_index`, `stage_rows`.
Configuration fields: `configurationId`, `instrumentId`, `allocationId`,
`timeframeMinutes`. Enumeration: `configurationId`, `resultId`, `stageId`, `status`.
Public HO2 rows are withheld; access returns a withheld marker, no financial row.
Retained results contain `configurationId`, `resultId`, `stageId`, `status`,
`metrics`, `execution`; curve rows contain the first three fields and `curve`.
Price rows contain `instrumentId`, `stageId`, `price`. `raw_member(name)` returns
CRC/SHA-verified gzip bytes. The chunk index exposes names, scopes, result IDs,
encoded/decoded sizes and hashes.

Opening validates all chunks sequentially with bounded memory. Subsequent
enumeration decodes no curves; access decodes only the selected chunk. Archive
offsets are reused. This is not lazy opening or native execution revalidation;
there is no full-array cache, replay, pack read or current-source-pin requirement.

Stored `study.request` must be an object with three stage objects, each containing
an object protocol. The loader checks these shapes before request normalization:
stored request/protocol paths never cause an external read or filesystem probe.
This applies to sealed verification/public export and the writer's unsealed check.
New execution requests still accept request files and relative protocol paths.

### Concrete synthetic reader output

The following excerpts come from the native synthetic example's public artifact,
configuration `oi` for `BYBIT_AAAUSDT`, target TF 30 minutes. These are artificial
prices executed by native accounts, not real market results or capacity fixtures.
The result excerpt omits `execution` and other metric fields; curve and price
wrappers below are complete. Variables shorten the repeated exact IDs:

```python
cid = "0edce3f961346bd97d7ef36dda996ae15d904b4213f976bdf03d0e3d07f83c43"
rid = "3171e68fd8d8e9fc971b27f58307facb45ae8499180a19f67382e9f1e355f557"

# Excerpt of stored.result(cid, "IS"):
{
    "configurationId": cid, "resultId": rid, "stageId": "IS", "status": "complete",
    "metrics": {
        "metricsVersion": "configuration-metrics.1", "initialCapital": 10000.0,
        "finalCapital": 13107.38766635553, "returnPct": 31.0738766635553, "trades": 7,
    },
}

# Complete stored.curve(cid, "IS"):
{
    "configurationId": cid, "resultId": rid, "stageId": "IS",
    "curve": {
        "curveVersion": "configuration-curves.1", "valueScale": 100,
        "timeMs": [1577894400000, 1578096000000],
        "equityQ": [0, 3107], "balanceQ": [0, 3107], "cashGaps": [],
        "status": "available", "endpoint": "observedClose",
    },
}

# Complete stored.price("BYBIT_AAAUSDT", "IS"):
{
    "instrumentId": "BYBIT_AAAUSDT", "stageId": "IS",
    "price": {
        "curveVersion": "configuration-curves.1", "valueScale": 100,
        "status": "available", "anchorPrice": 128.0, "anchorTimeMs": 1577894400000,
        "lastObservedTimeMs": 1578096000000, "coverageGaps": [],
        "sourceFingerprint": "224392314aefe3c85dc685d76ee531fbcdda097a5167ec68d889ec04bce4e3fd",
        "timeMs": [1577894400000, 1577923200000, 1578009600000, 1578096000000],
        "priceQ": [0, 0, 0, 0],
    },
}

# Complete stored.result(cid, "HO2") on the public projection:
{"status": "withheld", "configurationId": cid, "stageId": "HO2"}
```

Times are UTC epoch milliseconds. Capital is in account quote-currency units;
`returnPct` is full-precision percent. Divide Q values by `valueScale` to obtain
percent returns: 3107 means 31.07%, not capital or a fraction of one. `anchorPrice`
is the actual first base-bar open; priceQ expresses its uncosted percentage change.
These periodic synthetic prices return to the anchor at the displayed boundaries.
Never substitute display samples for the exact financial metrics. Withheld
responses contain no financial fields, resultId or curve/price wrapper.

Configuration IDs hash normalized allocation without allocationId, instrument,
used sources and resolved used auxiliary bindings. Snapshot aliases/content pins
are excluded from logical IDs. Result IDs add stage and exact bounds. Separate
specification, consumed-execution and implementation digests preserve attribution.
Implementation includes native source closure, this package, canonical metrics,
Python and libraries. Public retains specification/implementation IDs, derives
retained-data/artifact IDs and records owner completion SHA-256 as lineage.

Metrics version `configuration-metrics.1` preserves native financial sums/counts,
adds full-resolution equity/balance DD, canonical observed bar-open UTC-day Sharpe
(explicit millisecond timestamps), calendar month exposure, trades/month and
annualization. Valid Sharpe series retain counts even without a ratio; no trades
has PF status `noTrades`. Annualization is unavailable at <=30 days; the short-span
marker covers 31..<90 days. No observations retains capital facts but marks
performance unavailable. `validation.py` declares the full metric field set.

## Physical schema

Exactly six files: `study.json` (`pattern-lab.configuration-study.v1`, formatVersion
1), `index.json` (`pattern-lab.configuration-index.v1`), `results.zip`,
`provenance.json`, `status.json`, and last-written `completion.json`
(`pattern-lab.configuration-completion.v1`). Completion hashes the other five.
Root JSON <=16 MiB each. Ordinary ZIP_STORED <=128 MiB, <=65,534 members; no ZIP64,
descriptors, comments, extras, directories or links. Exact names, contiguous
extents, local/central headers, CRC/SHA, sizes, IDs/counts and types are checked.
The central-directory length must equal the sum of `46 + ASCII name length` for
its entries; trailing ZIP64 records or padding are refused without scanning payloads.

Normalization preflights `3 * instruments * (2 * sum(ceil(A[tf] / 16)) + 1)`
base members, rounding each target-TF allocation group separately and including
one shared price member per instrument/stage. The limit is 65,534. Four instruments
with 200 allocations at each of four TFs need 1,260 base members. This is a lower
bound when byte limits force extra splits: actual writer and JSON/byte guards
still apply; admission does not guarantee that every byte budget will fit.

Order: stage IS/HO1/HO2, lexical instrument, numeric target TF, lexical config ID.
Each group emits metrics/curves pairs of <=16 results (earlier split only for the
8 MiB decoded chunk bound). Names:
`chunks/STAGE/sha256([instrument,tf])/NNNNN.{metrics,curves}.json.gz`.
After every account pair, all prices follow sorted by instrument then lexical
stage (HO1, HO2, IS): `price/sha256(instrument)/STAGE.json.gz`. Public omits HO2.
Chunk schema is `pattern-lab.configuration-chunk.v1`; metrics/price have `records`,
curves also have deduplicated `timeGrids` addressed by record `gridId`.

Exactly one gzip member per chunk: level 9, mtime 0, empty filename, header
`1f8b08000000000002ff`. Bounded decoding rejects trailing/concatenated/truncated
data. Total decoded bytes <=512 MiB. Public copies retained gzip bytes unchanged.
Independently generated gzip may differ by zlib; saved bytes verify cross-host.

Curves: `configuration-curves.1`, valueScale 100, JS-safe integers, ties-to-even
quantization. Account points: initial zero-return anchor, latest actual close at
each three-day boundary, actual final close, and validated flat-cash end extension
if necessary. `cashGaps` gives exact start/end/cashQ intervals and support points.
Prices use actual base-5m first open, UTC-midnight closes or null, actual last close
and coverage gaps, never flat cash. These samples cannot recompute full metrics.

## Bounded execution and limitations

One existing AccountJob per allocation. Coordinator reads packs/publishes;
preparation is scoped to instrument/TF/stage. At most W jobs remain submitted but
not canonically consumed; later completions hold credit. Full native records are
extracted immediately and released; compact buffer <=W+16. Existing spawn workers
and child thread bounds apply. Source checks surround execution and sealing.
Progress writes are <=once/second except start/end/failure. Errors expose phase,
coordinates, type and reason code, never arbitrary exception financial payloads.

Public removes HO2 financial payloads but is not a security boundary: the separate
owner artifact still contains HO2. Synthetic capacity artifacts carry
`synthetic=true`, `kind=syntheticFixture`, null native IDs/validation and no native
input/rule receipts. No production CLI bypass exists. Verification certifies
compact integrity/consistency, not full native evidence or financial replay.
