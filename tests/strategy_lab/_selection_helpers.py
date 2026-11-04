"""Per-instrument synthetic X fixtures; no market data or ignored probes."""
from copy import deepcopy
import json
import numpy as np
import pandas as pd
import pytest

from data_provider import data, manifest
from tools.strategy_lab.synthetic_parquet import synthetic_rules
from _filter_helpers import fixed_request, is_request, publish_filter, save_request
from tools.strategy_lab.selection import prepare_ranking, open_ranking, write_fixed_request
from tools.strategy_lab.fixed.run import run
from tools.strategy_lab.generate import generate_dataset


def publish(root, *, count=3, late=None, tie=False, future=None, drop=False):
    times = pd.date_range("2023-12-01", "2024-09-01", freq="5min", inclusive="left", tz="UTC")
    slots = np.arange(len(times))
    instruments = []
    for i in range(count):
        symbol = chr(65+i)*3 if count <= 26 else f"T{i:02}"
        phase = 0 if tie and i == 1 else i
        opening = 100+(6+phase)*np.sin(slots/(24+phase*3))
        close = 100+(6+phase)*np.sin((slots+1)/(24+phase*3))
        values = np.column_stack((opening, np.maximum(opening, close)+.2+phase*.3,
            np.minimum(opening, close)-.2-phase*.3, close, np.ones(len(times))))
        if future is not None:
            values[times >= pd.Timestamp(future, tz="UTC"), 1] += .7*(i+1)
        rules = synthetic_rules(symbol)
        mask = np.ones(len(times), dtype=bool)
        if late is not None and i == count-1:
            rules["listed_at_utc"] = pd.Timestamp(late, tz="UTC").isoformat()
            rules["raw_contract_fields"]["launchTime"] = str(pd.Timestamp(late, tz="UTC").value//10**6)
            mask &= times >= pd.Timestamp(late, tz="UTC")
        if drop and i == 0:
            mask &= ~((times >= pd.Timestamp("2024-02-01", tz="UTC")) & (times < pd.Timestamp("2024-02-03", tz="UTC")))
        instruments.append(data.InstrumentSource(symbol=symbol, venue="BYBIT", contract=symbol+"USDT", quote_currency="USDT",
            roles=["research_only"], timestamps=times.asi8[mask]//10**6, ohlcv=values[mask], instrument_rules=rules,
            source=manifest.build_source_metadata(input_format="synthetic", input_dtype="float64",
                source_reference="X varied instrument fixture", volume_unit_evidence="synthetic"),
            verification=manifest.build_verification(volume_quote_verified=True, volume_quote_evidence="synthetic",
                closed_before_utc="2024-09-01T00:00:00Z", closure_evidence="synthetic", closure_source="test")))
    data.publish_pack(root, instruments, universe=manifest.build_universe())
    return [s.venue+"_"+s.contract for s in instruments]


def document(root, name, value):
    path = root/name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def request(*, mode="fixed", start=None, end="2024-06-15", cadence=2, k=2, draws=32):
    return dict(schema_version="strategy_lab_x_request_v1", name="varied-x",
        calendar=dict(mode=mode, start=start or ("2024-03-01" if mode == "fixed" else "2024-02-01"), end=end,
            tradingTimeframeMinutes=60, **(dict(isMonths=1, oosMonths=1) if mode == "is_oos" else {})),
        tickerSelector=dict(version=1, enabled=True, method="meanBarRangePct", methodVersion=1, topN=k,
            rebalanceMonths=cadence, evaluateUnselected=False, params=dict(lookbackMonths=1, timeframeMinutes=30)),
        controls=dict(seed=17, draws=draws))


def universe(root, tickers):
    return document(root, "universe.json", dict(schema_version="strategy_lab_universe_v1", name="varied",
        evidence_classification="exploratory", selection_note="synthetic", tickers=tickers))


@pytest.fixture(scope="module")
def selection_family(tmp_path_factory):
    work = tmp_path_factory.mktemp("selection-family")
    pack, filters = work/"pack", publish_filter(work/"filters", start="2023-12-01", end="2024-09-01")
    tickers = publish(pack)
    u = universe(work, tickers)
    rankings, fixed_roots, candidate_roots = {}, {}, {}
    for mode in ("fixed", "is_oos"):
        rankings[mode] = work/(mode+"-ranking")
        prepare_ranking(document(work, mode+".json", request(mode=mode)), rankings[mode], ticker_list=u, data_root=pack)
    for label, filter_root in (("off", None), ("on", filters)):
        raw = fixed_request(filter_root)
        raw["generation"]["market_data"]["instruments"] = [dict(instrument_id=t, cell="research") for t in tickers]
        raw["generation"]["expected_run_count"] = 3
        template = document(work, label+"-template.json", raw)
        transform = work/(label+"-transform")
        write_fixed_request(rankings["fixed"], template, transform, warmup_bars=100)
        fixed_roots[label] = work/(label+"-fixed")
        run(transform/"fixed-request.json", pack, fixed_roots[label])
        raw = is_request(filter_root)
        if label == "off":
            raw["schema_version"] = "strategy_lab_runspec_v2"
            raw["generation"]["execution"] = dict(policy="risk_bracket_v1", entry_filters=[])
        raw["generation"]["market_data"]["instruments"] = [dict(instrument_id=t, cell="research") for t in tickers]
        raw["generation"]["windows"].update(requested_end="2024-07-01", expected_window_count=4)
        raw["preregistration"]["analysis_scopes"][0]["window_numbers"] = [1, 2, 3, 4]
        candidate_roots[label] = work/(label+"-candidates")
        generate_dataset(document(work, label+"-is.json", raw), data_root=pack, output_dir=candidate_roots[label])
    return dict(work=work, pack=pack, filters=filters, tickers=tickers, universe=u, rankings=rankings,
        fixed=fixed_roots, candidates=candidate_roots)
