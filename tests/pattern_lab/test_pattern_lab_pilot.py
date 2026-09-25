"""M5 public extension semantics and caller-owned evidence regression tests."""
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tools.pattern_lab import PatternLabDataError, analysis, study
from tools.pattern_lab.examples import pilot_extension as pilot
from tools.pattern_lab.study import evidence, report
from ._bracket_helpers import build
from ._helpers import ANCHOR_MS
from tools.pattern_lab.manifest import format_epoch_ms
from . import _helpers as h
from tools.pattern_lab.study import validation


def test_nonempty_pilot_direct_spawn_persistence_and_ownership(tmp_path):
    # Only closes 103, 109 and 139 exceed their preceding high. Their next
    # bars return -1/4, +1/4 and unavailable respectively, independently of RMS.
    bars = [(100,101,99,100,10), (100,104,99,103,10), (100,101,74,75,10),
            (80,130,79,109,10), (100,126,99,125,10), (100,126,99,100,10),
            (100,140,99,139,10), (100,101,99,100,10), (100,101,99,100,10)]
    stamps, values = h.timeframe_bars(30, bars, drop_groups=[7])
    h.publish(tmp_path / "pack", [h.instrument_source(stamps, values),
        h.instrument_source(stamps, values, symbol="BBB", contract="BBB-USDT-SWAP")])
    request = validation.external_document(h.normalized_study(start_group=1, end_group=9,
        models=[h.fixed_horizon_model(30, [30], directions=["long"], commission_pct_per_side=0)]))
    request.update(schema_version=2, context={}, execution={"kind":"development"})
    request["extensions"] = [{"module":"pilot_extension", "source_root":str(Path(pilot.__file__).parent), "helpers":[]}]
    request["hypotheses"] = [{"id":"breakout", "hypothesis":"prior_high_breakout", "parameters":{"lookback":1}, "occurrence":"every_qualifying_bar"}]
    request["metrics"] = [{"id":"downside", "metric":"downside_rms"}]
    runs = []
    for workers in (1, 2):
        root = tmp_path / str(workers)
        study.run_study(request=request, data_root=tmp_path / "pack", output_root=root, workers=workers)
        saved = study.load_results(root)
        metric = study.summarize_results(saved)["metrics"]["values"]
        assert len(metric) == 1 and metric[0]["availability"] == "available"
        assert metric[0]["value"] == np.sqrt((.25 ** 2) / 2)
        instance = saved.family["models"][0]
        case = saved.case("fh", 30, instance["cases"]["30"][0]["case_id"])
        tables = []
        for identifier in saved.completed_instruments:
            reader = saved.instrument_reader(identifier)
            emitted = reader.table("emissions")
            assert emitted.anchor_open_ms.tolist() == [ANCHOR_MS + i * 1800000 for i in (1,3,6)]
            observations = reader.observations(instance=instance, case=case, timeframe_minutes=30, variant_id="breakout")
            assert observations.return_valid.tolist() == [True, True, False]
            assert observations.net_return.iloc[:2].tolist() == [-.25, .25]
            tables.append([reader.table(name) for name in ("conditions", "episodes", "emissions", "primitives")])
            public = reader.table("primitives"); public.loc[:, "exit_price"] = -999
            public = reader.evidence_table(instance); public.loc[:, "exit_price"] = -999
            pd.testing.assert_frame_equal(observations, reader.observations(instance=instance, case=case, timeframe_minutes=30, variant_id="breakout"))
            reader.release()
        study.regenerate_report(root)
        assert study.summarize_results(study.load_results(root))["metrics"]["values"] == metric
        runs.append((saved, tables, metric))
    assert runs[0][0].provenance["identities"] == runs[1][0].provenance["identities"]
    assert runs[0][2] == runs[1][2]
    for direct, spawned in zip(runs[0][1], runs[1][1]):
        for left, right in zip(direct, spawned): pd.testing.assert_frame_equal(left, right)


def series(high, close=None, slots=None):
    high = np.asarray(high, dtype=float)
    close = high if close is None else np.asarray(close, dtype=float)
    slots = np.arange(len(high)) if slots is None else np.asarray(slots)
    values = np.column_stack((close, high, close-1, close, np.ones(len(high))))
    return study.BarSeries("TEST_X",30,1800000,slots*1800000,slots,values,0)


def test_breakout_threshold_warmup_gap_and_future():
    bars = series([10,11,100,12,13,14,15], [9,10,12,11,14,15,16], [0,1,2,4,5,6,7])
    result = pilot.breakout(bars,{"lookback":2},{})
    assert result.valid.tolist() == [False,False,True,False,False,True,True]
    assert result.value.tolist() == [False,False,True,False,False,True,True]
    equal = pilot.breakout(series([10,11,100],[9,10,11]),{"lookback":2},{})
    assert equal.valid[-1] and not equal.value[-1]
    below = pilot.breakout(series([10,11,100],[9,10,10]),{"lookback":2},{})
    assert below.valid[-1] and not below.value[-1]
    changed = bars.values.copy(); changed[3:] *= 1000
    future = pilot.breakout(replace(bars, values=changed, research_start_index=2),{"lookback":2},{})
    np.testing.assert_array_equal(result.value[:3],future.value[:3])
    np.testing.assert_array_equal(result.valid[:3],future.valid[:3])


@pytest.mark.parametrize("count",[0,1,20,21,400])
def test_breakout_independent_loop_oracle(count):
    rng = np.random.default_rng(702)
    bars = series(rng.uniform(90,110,count),rng.uniform(90,110,count),np.cumsum(rng.choice([1,2],count,p=[.98,.02])))
    result = pilot.breakout(bars,pilot.parameters({}),{})
    values, valid = np.zeros(count,bool), np.zeros(count,bool)
    for t in range(20,count):
        valid[t] = all(bars.slots[k]-bars.slots[k-1]==1 for k in range(t-19,t+1))
        values[t] = valid[t] and bars.close[t] > max(bars.high[t-20:t])
    np.testing.assert_array_equal(result.value,values)
    np.testing.assert_array_equal(result.valid,valid)


@pytest.mark.parametrize("value",[True,False,0,-1,2.0,"20",None,[],{}])
def test_breakout_rejects_invalid_lookback(value):
    with pytest.raises(PatternLabDataError,match="positive integer"):
        pilot.parameters({"lookback":value})


def test_downside_rms_mask_denominator_and_null():
    frame = pd.DataFrame({"net_return":[-.1,-.2,.3,0,np.inf,np.nan],"return_valid":[True]*4+[False]*2})
    assert pilot.downside_rms(frame) == pytest.approx(np.sqrt(.05/4))
    assert pilot.downside_rms(frame.iloc[2:4]) == 0
    assert pilot.downside_rms(frame.iloc[4:]) is None
    assert pilot.downside_rms(frame.iloc[:0]) is None
    for bad in [np.inf,-np.inf,np.nan]:
        with pytest.raises(PatternLabDataError,match="finite"):
            pilot.downside_rms(pd.DataFrame({"net_return":[bad],"return_valid":[True]}))


def test_public_reader_ownership_extension_analysis_and_saved_report(tmp_path,monkeypatch):
    request = build(tmp_path/"pack")
    request["study"]["start_utc"] = format_epoch_ms(ANCHOR_MS+20*1800000)
    request["extensions"] = [{"module":"pilot_extension","source_root":str(Path(pilot.__file__).parent),"helpers":[]}]
    request["hypotheses"].append({"id":"breakout20","hypothesis":"prior_high_breakout","parameters":{},"occurrence":"every_qualifying_bar"})
    request["metrics"] = [{"id":"downside","metric":"downside_rms"}]
    root = Path(study.run_study(request=request,data_root=tmp_path/"pack",output_root=tmp_path/"run")["run_root"])
    saved = study.load_results(root)
    reader = saved.instrument_reader(saved.completed_instruments[0])
    instance = next(m for m in saved.family["models"] if m["model_id"]=="fixed_horizon_path")
    case = saved.case(instance["model_instance_id"],30,instance["cases"]["30"][0]["case_id"])
    select = dict(instance=instance,case=case,timeframe_minutes=30)
    before = reader.observations(**select)
    public = reader.table("primitives"); public["exit_price"] *= 1.1
    public = reader.evidence_table(instance); public["exit_price"] *= 2
    anchors = reader.eligible_anchors(); anchors[30][:] = -1; anchors.clear()
    pd.testing.assert_frame_equal(before,reader.observations(**select))
    assert (reader.eligible_anchors()[30]>=0).all()
    reader.sequential_keys()  # shared validation before counting requested copies
    copies = []
    original = pd.DataFrame.copy
    def counted(frame,*args,**kwargs):
        copies.append(id(frame)); return original(frame,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(pd.DataFrame,"copy",counted)
        path = reader.table("sequential_path")
    assert copies == [id(reader._tables["sequential_path"])]
    path.loc[0,"balance"] = -123
    assert reader.table("sequential_path").loc[0,"balance"] != -123
    reader.release()
    summary = study.summarize_results(saved)
    values = summary["metrics"]["values"]
    # Increasing fixture closes never exceed the previous high (equality), hence no breakout events.
    empty = [v for v in values if v["variant_id"]=="breakout20"]
    assert empty and all(v["value"] is None and v["availability"]=="available" for v in empty)
    destination = tmp_path/"analysis"
    analysis.run_analysis(request={"schema_version":2,"analysis_name":"synthetic pilot","model_instances":[instance["model_instance_id"]],"pairwise":[]},run_root=root,output_root=destination)
    analysis.load_analysis(destination)
    raw = {p:evidence.file_digest(p) for p in (root/"jobs").rglob("*") if p.is_file()}
    study.regenerate_report(root); analysis.regenerate_report(destination)
    assert raw == {p:evidence.file_digest(p) for p in raw}
    assert "downside_rms" in (root/evidence.REPORT_FILE).read_text(encoding="utf-8")
    facts = report._fact_table({"requested_end_ms":0,"last_observed_close_ms":None},caption="<test>")
    assert "1970-01-01T00:00:00Z" in facts and "unavailable" in facts and "&lt;test&gt;" in facts
    assert "30m" in report._account_label(summary["sequential_accounts"][0])
    summary["sequential_accounts"][0]["coverage"].update(tail_complete=False,missing_tail_slots=None)
    html = report._sequential_report(summary)
    assert "slot count unavailable" in html and "None grid slots" not in html
