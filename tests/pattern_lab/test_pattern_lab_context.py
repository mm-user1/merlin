"""Independent context arithmetic and coordinator boundaries on synthetic bars."""
import copy
import json
from dataclasses import replace
from types import MappingProxyType

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabStudyError, study
from tools.pattern_lab.study import context, contracts, job, runner, validation
from . import _helpers as h


def context_fixture(root, *, suffix=0, change=False, drop=(), target_drop=(), future_change=False):
    stamps, values = h.synthetic_series(240 + suffix)
    other = values.copy()
    other[:, 3] += (np.arange(len(other)) % 3 == 0) * 2
    if change:
        other[30, 3] += .5
    other[:, 1] = np.maximum(other[:, 1], other[:, 3]) + 1
    if future_change:
        other[150:, :4] *= 2
    keep = ~np.isin(np.arange(len(stamps)), drop)
    target_keep = ~np.isin(np.arange(len(stamps)), target_drop)
    h.publish(root, [h.instrument_source(stamps[target_keep], values[target_keep]),
                    h.instrument_source(stamps[keep], other[keep], symbol="BTC", contract="BTC-USDT-SWAP", roles=["factor"])])
    document = validation.external_document(h.normalized_study(start_group=2, end_group=35))
    document.update(schema_version=2, context={"btc": {"ids": ["TEST_BTC-USDT-SWAP"]}},
                    execution={"kind": "development"})
    document["hypotheses"] = [{"id": "btc", "hypothesis": "two_green_volume_btc",
                                "parameters": {"alias": "btc"}, "occurrence": "state_entry"}]
    return document


def grid(close, opens, valid):
    stamps = np.arange(len(close), dtype=np.int64) * 300000
    values = np.column_stack([opens, np.maximum(opens, close)+1, np.minimum(opens, close)-1, close, np.ones(len(close))])
    bars = contracts.BarSeries("X", 5, 300000, stamps, stamps//300000, values, 0)
    return context.ContextSeries(bars, np.asarray(valid, dtype=bool))


def test_arithmetic_gaps_prefix_and_doji():
    a = grid([10., 11., 11., 9., 12.], [9., 10., 11., 10., 11.], [1,1,0,1,1])
    b = grid([10., 11., 11., 9., 12.], [10., 12., 10., 8., 11.], [1,1,1,1,1])
    g = context.ContextGrid(5, a.bars.timestamps_ms, {"btc": {"a": a}, "panel": {"a": a, "b": b}})
    btc = context.btc_return(g, {"alias": "btc"}, {})
    np.testing.assert_array_equal(btc.valid, [0,1,0,0,1])
    np.testing.assert_allclose(btc.values[btc.valid], [.1, 1/3])
    panel = context.panel_green(g, {"alias": "panel"}, {})
    np.testing.assert_allclose(panel.values, [.5,.5,.5,.5,1])
    np.testing.assert_array_equal(panel.valid, [1,1,0,1,1])
    assert not (panel.values[:2] > .5).any()
    a.bars.values[-1, 3] = 900
    np.testing.assert_array_equal(context.btc_return(g, {"alias":"btc"}, {}).values[:-1], btc.values[:-1])
    np.testing.assert_array_equal(context.panel_green(g, {"alias":"panel"}, {}).values[:-1], panel.values[:-1])


def test_dense_three_bar_lookback_rewarms_after_missing_slot():
    series = grid([10., 11., 12., np.nan, 14., 15., 16., 17.],
                  [9., 10., 11., np.nan, 13., 14., 15., 16.], [1, 1, 1, 0, 1, 1, 1, 1])
    # Dense slots remain adjacent across the missing observation.
    assert series.bars.contiguous_with_previous()[3:5].all()
    values = np.full(8, np.nan)
    valid = np.zeros(8, dtype=bool)
    for i in range(2, 8):  # required_prior_bars=2, including all three observations
        valid[i] = series.valid[i-2:i+1].all()
        if valid[i]:
            values[i] = series.bars.close[i] / series.bars.close[i-2] - 1
    np.testing.assert_array_equal(valid, [0, 0, 1, 0, 0, 0, 1, 1])
    np.testing.assert_allclose(values[valid], [.2, 16/14-1, 17/15-1])
    actual = context.btc_return(
        context.ContextGrid(5, series.bars.timestamps_ms, {"btc": {"source": series}}),
        {"alias": "btc"}, {})
    np.testing.assert_array_equal(actual.valid, [0, 1, 1, 0, 0, 1, 1, 1])
    np.testing.assert_allclose(actual.values[actual.valid], [.1, 1/11, 1/14, 1/15, 1/16])


def test_context_run_identity_and_single_preparation(tmp_path, monkeypatch):
    doc = context_fixture(tmp_path / "pack")
    calls = []
    original = context.prepare
    def counted(*args):
        calls.append(len(args[1]))
        return original(*args)
    monkeypatch.setattr(context, "prepare", counted)
    result = study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"run")
    loaded = study.load_results(tmp_path/"run")
    assert loaded.complete and loaded.completion["schema_version"] == 2
    assert calls == [1]
    facts = json.loads((tmp_path/"run"/"context.json").read_text())
    assert len(facts["features"]) == 1 and facts["raw_context_bytes"] > facts["output_bytes"] > 0
    from tools.pattern_lab.analysis.source import admit_source
    binding = admit_source(tmp_path/"run", model_instances=["fh"]).binding_document()
    assert binding["schema_version"] == 2 and binding["semantic_inputs"]["context"] == doc["context"]
    for name, suffix, change in [("append", 12, False), ("edit", 0, True)]:
        context_fixture(tmp_path/name, suffix=suffix, change=change)
        rerun = study.run_study(request=doc, data_root=tmp_path/name, output_root=tmp_path/(name+"run"))
        assert (result["identities"]["data_input_sha256"] == rerun["identities"]["data_input_sha256"]) is (not change)


def test_admission_and_context_failure(tmp_path, monkeypatch):
    doc = context_fixture(tmp_path/"pack")
    bad = copy.deepcopy(doc)
    bad["context"]["unused"] = {"ids": [h.STUDY_INSTRUMENT]}
    with pytest.raises(PatternLabDataError, match="unused"):
        study.run_study(request=bad, data_root=tmp_path/"pack", output_root=tmp_path/"bad")
    assert not (tmp_path/"bad").exists()
    def fail(*args):
        raise ValueError("context feature intentional failure")
    monkeypatch.setattr(context, "prepare", fail)
    with pytest.raises(PatternLabStudyError, match="intentional failure"):
        study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"failed")
    status = json.loads((tmp_path/"failed"/"status.json").read_text())
    assert status["failure"]["phase"] == "context"
    assert status["failure"]["instrument_id"] is None
    assert not (tmp_path/"failed"/"completion.json").exists()


def test_missing_cache_and_edited_normalized(tmp_path):
    doc = context_fixture(tmp_path/"pack")
    normalized = validation.validated_request(doc)
    normalized.execution["kind"] = "validation"
    with pytest.raises(PatternLabDataError, match="candidate"):
        validation.validated_request(normalized)
    series = grid([1.,2.], [1.,1.], [1,1]).bars
    with pytest.raises(PatternLabDataError, match="missing prepared context"):
        job._feature_value(series, contracts.FeatureRequest("btc_close_return", {"alias":"btc", "threshold":0.}), {})


@pytest.mark.parametrize("workers", [1, 2])
def test_worker_context_equivalence(tmp_path, workers):
    doc = context_fixture(tmp_path/"pack", drop=[30])
    first = study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"first")
    second = study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"second", workers=workers)
    assert first["identities"] == second["identities"]
    left, right = [study.load_results(tmp_path/name).instrument_reader(h.STUDY_INSTRUMENT) for name in ("first","second")]
    import pandas as pd
    for name in ("conditions", "emissions", "primitives"):
        pd.testing.assert_frame_equal(left.table(name), right.table(name))


def test_custom_context_prefix_and_readonly_transport(tmp_path, monkeypatch):
    from pathlib import Path
    from tools.pattern_lab.examples import context_extension as example
    a = grid([10., 11., 12., 13.], [9.,10.,11.,12.], [1,1,0,1])
    complete = context.ContextGrid(5, a.bars.timestamps_ms, {"btc":{"x":a}})
    result = example.evaluate(complete, {"alias":"btc"}, {})
    np.testing.assert_array_equal(result.valid, [0,1,0,0])
    prefix_series = grid([10.,11.], [9.,10.], [1,1])
    prefix = example.evaluate(context.ContextGrid(5, prefix_series.bars.timestamps_ms,
                              {"btc":{"x":prefix_series}}), {"alias":"btc"}, {})
    np.testing.assert_allclose(prefix.values, result.values[:2])
    doc = context_fixture(tmp_path/"pack")
    doc["extensions"] = [{"module":"context_extension", "source_root":str(Path(example.__file__).parent), "helpers":[]}]
    doc["hypotheses"] = [{"id":"custom", "hypothesis":"example_context_positive", "parameters":{"alias":"btc"},
                           "occurrence":"state_entry"}]
    original = runner.run_instrument_job
    def inspect(payload):
        import pickle
        received = pickle.loads(pickle.dumps(payload))
        job.protect_inputs(received)
        for prepared in received.timeframes:
            assert prepared.context_features
            for value in prepared.context_features.values():
                assert not value.values.flags.writeable and not value.valid.flags.writeable
                assert value.values.ndim == 1
        return original(payload)
    monkeypatch.setattr(runner, "run_instrument_job", inspect)
    study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"run")


def test_scope_cycle_and_undeclared_helper(tmp_path, monkeypatch):
    monkeypatch.setattr(contracts, "_REGISTRY", {key:dict(value) for key,value in contracts._REGISTRY.items()})
    evaluate = lambda grid, params, deps: None
    contracts.register_feature(contracts.FeatureDescriptor("target_test", "1", evaluate))
    contracts.register_feature(contracts.FeatureDescriptor("context_test", "1", evaluate, scope="context",
        dependencies=lambda p:(contracts.FeatureRequest("target_test"),)))
    with pytest.raises(PatternLabDataError, match="cannot depend"):
        contracts.resolve_feature_closure([contracts.FeatureRequest("context_test")], where="test")
    contracts.register_feature(contracts.FeatureDescriptor("cycle_test", "1", evaluate, scope="context",
        dependencies=lambda p:(contracts.FeatureRequest("cycle_test"),)))
    with pytest.raises(PatternLabDataError, match="cycle"):
        contracts.resolve_feature_closure([contracts.FeatureRequest("cycle_test")], where="test")
    with pytest.raises(PatternLabDataError, match="scope"):
        contracts.register_hypothesis(contracts.HypothesisDescriptor("wrong_scope", "1", evaluate, scope="context"))
    doc = context_fixture(tmp_path/"pack")
    (tmp_path/"ctx_unlisted_helper.py").write_text("VALUE = 1\n")
    (tmp_path/"ctx_bad_extension.py").write_text("from ctx_unlisted_helper import VALUE\ndef register(context): pass\n")
    doc["extensions"] = [{"module":"ctx_bad_extension", "source_root":str(tmp_path), "helpers":[]}]
    with pytest.raises(PatternLabDataError, match="undeclared local helper"):
        validation.validated_request(doc)


def test_context_interrupt_and_metadata_coverage(tmp_path, monkeypatch):
    doc = context_fixture(tmp_path/"pack")
    bad = copy.deepcopy(doc)
    bad["study"]["warmup_start_utc"] = h.utc(h.ANCHOR_MS-1800000)
    bad["protocol"]["earliest_warmup_start_utc"] = bad["study"]["warmup_start_utc"]
    with pytest.raises(PatternLabDataError, match="coverage starts"):
        study.run_study(request=bad, data_root=tmp_path/"pack", output_root=tmp_path/"bad")
    assert not (tmp_path/"bad").exists()
    def stop(*args): raise KeyboardInterrupt()
    monkeypatch.setattr(context, "prepare", stop)
    with pytest.raises(KeyboardInterrupt):
        study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"interrupted")
    status = json.loads((tmp_path/"interrupted"/"status.json").read_text())
    assert status["terminal_status"] == "interrupted" and status["failure"]["phase"] == "context"


def test_panel_alias_overlap_one_read_and_readonly_grid(tmp_path, monkeypatch):
    from tools.pattern_lab import data
    doc = context_fixture(tmp_path/"pack")
    doc["context"]["panel"] = {"ids":["TEST_BTC-USDT-SWAP", h.STUDY_INSTRUMENT]}
    doc["hypotheses"].append({"id":"panel", "hypothesis":"two_green_volume_panel", "parameters":{"alias":"panel"},
                             "occurrence":"every_qualifying_bar"})
    reads = []
    original = data._ReadSession.load_slice
    def counted(self, identifier, **kwargs):
        reads.append(identifier)
        return original(self, identifier, **kwargs)
    monkeypatch.setattr(data._ReadSession, "load_slice", counted)
    registration = contracts.registration("feature", "panel_green_fraction")
    evaluator = registration.descriptor.evaluate
    def protected(grid, params, features):
        with pytest.raises(TypeError): grid.sources["oops"] = {}
        with pytest.raises(TypeError): params["threshold"] = .2
        for source in grid.sources["panel"].values():
            assert not source.bars.values.flags.writeable and not source.valid.flags.writeable
        return evaluator(grid, params, features)
    monkeypatch.setitem(contracts._REGISTRY["feature"], "panel_green_fraction",
                       replace(registration, descriptor=replace(registration.descriptor, evaluate=protected)))
    study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"run")
    assert reads.count("TEST_BTC-USDT-SWAP") == 1
    assert reads.count(h.STUDY_INSTRUMENT) == 2  # one bounded read per role
    admitted = json.loads((tmp_path/"run"/"spec"/"context.json").read_text())
    assert admitted["self_inclusion"] == [h.STUDY_INSTRUMENT]


def test_target_source_gaps_prefix_conditions_and_unknown_controls(tmp_path):
    import pandas as pd
    from tools.pattern_lab import analysis
    doc = context_fixture(tmp_path/"pack", drop=[30], target_drop=[18])
    roots = []
    for name, document, pack in (("full", doc, tmp_path/"pack"),
        ("prefix", {**doc, "study":{**doc["study"], "end_utc":h.utc(h.study_group_ms(20))}}, tmp_path/"pack")):
        root = tmp_path/name
        study.run_study(request=document, data_root=pack, output_root=root)
        roots.append(root)
    context_fixture(tmp_path/"future-pack", drop=[30], target_drop=[18], future_change=True)
    study.run_study(request=doc, data_root=tmp_path/"future-pack", output_root=tmp_path/"future")
    readers = [study.load_results(root).instrument_reader(h.STUDY_INSTRUMENT) for root in (*roots,tmp_path/"future")]
    cutoff = h.study_group_ms(20)-1800000
    for table in ("conditions", "emissions"):
        frames = [reader.table(table) for reader in readers]
        prefixes = [frame.loc[frame.anchor_open_ms < cutoff].reset_index(drop=True) for frame in frames]
        pd.testing.assert_frame_equal(prefixes[0], prefixes[1])
        pd.testing.assert_frame_equal(prefixes[0], prefixes[2])
    conditions = readers[0].table("conditions")
    assert (~conditions.valid).any()
    ar = {"schema_version":2, "analysis_name":"unknown context", "model_instances":["fh"], "pairwise":[]}
    analysis.run_analysis(request=ar, run_root=roots[0], output_root=tmp_path/"analysis")
    source = analysis.load_analysis(tmp_path/"analysis")
    assert source.strata().control_observations.sum() <= int((conditions.valid & ~conditions.value).sum()) * 2


def test_context_chain_and_instrument_transform_normalize_dependency_defaults(tmp_path):
    doc = context_fixture(tmp_path/"pack")
    (tmp_path/"ctx_nested.py").write_text('''
from tools.pattern_lab.study import contracts as c
def params(raw):
    return {"alias":raw["alias"]}
def aliases(p):
    return (p["alias"],)
def btc(p):
    return (c.FeatureRequest("btc_close_return", {"alias":p["alias"]}),)
def context_value(g,p,f):
    return f[btc(p)[0].key]
def context_dep(p):
    return (c.FeatureRequest("nested_context",p),)
def instrument_value(g,p,f):
    return f[context_dep(p)[0].key]
def instrument_dep(p):
    return (c.FeatureRequest("nested_instrument",p),)
def condition(g,p,f):
    value=f[instrument_dep(p)[0].key]
    return c.ConditionValue(value.values>0,value.valid.copy())
def register(ctx):
    ctx.register_feature(c.FeatureDescriptor("nested_context","1",context_value,validate_parameters=params,
        dependencies=btc,scope="context"))
    ctx.register_feature(c.FeatureDescriptor("nested_instrument","1",instrument_value,validate_parameters=params,
        dependencies=context_dep,prior_bars=lambda p:2))
    ctx.register_hypothesis(c.HypothesisDescriptor("nested_condition","1",condition,validate_parameters=params,
        dependencies=instrument_dep))
''', encoding="utf-8")
    doc["extensions"] = [{"module":"ctx_nested", "source_root":str(tmp_path), "helpers":[]}]
    doc["hypotheses"] = [{"id":"nested", "hypothesis":"nested_condition", "parameters":{"alias":"btc"},
                           "occurrence":"every_qualifying_bar"}]
    normalized = validation.validated_request(doc)
    assert normalized.variants[0].required_prior_bars == 2  # max, never 1+2
    study.run_study(request=normalized, data_root=tmp_path/"pack", output_root=tmp_path/"run")
    diagnostics = json.loads((tmp_path/"run"/"context.json").read_text())
    assert len(diagnostics["features"]) == 2


def test_declared_context_helper_mutation_is_caught(tmp_path, monkeypatch):
    from pathlib import Path
    from tools.pattern_lab.examples import context_extension
    doc = context_fixture(tmp_path/"pack")
    code = Path(context_extension.__file__).read_text().replace("example_context", "mutation_context")
    (tmp_path/"ctx_mutation.py").write_text(code, encoding="utf-8")
    helper = tmp_path/"ctx_helper.py"
    helper.write_text("VALUE = 1\n")
    doc["extensions"] = [{"module":"ctx_mutation", "source_root":str(tmp_path), "helpers":["ctx_helper.py"]}]
    doc["hypotheses"] = [{"id":"custom", "hypothesis":"mutation_context_positive", "parameters":{"alias":"btc"},
                           "occurrence":"state_entry"}]
    original = context.prepare
    def mutate(*args):
        result = original(*args)
        helper.write_text("VALUE = 2\n")
        return result
    monkeypatch.setattr(context, "prepare", mutate)
    with pytest.raises(PatternLabStudyError, match="context.*ctx_helper"):
        study.run_study(request=doc, data_root=tmp_path/"pack", output_root=tmp_path/"run")
    assert not (tmp_path/"run"/"completion.json").exists()


def test_raw_context_lifetime_does_not_depend_on_cyclic_gc(tmp_path, monkeypatch):
    import gc
    import weakref
    from tools.pattern_lab import data
    doc = context_fixture(tmp_path/"pack")
    request = validation.validated_request(doc)
    refs = []
    original = context.ContextSeries
    def track(*args):
        value = original(*args)
        refs.append(weakref.ref(value))
        return value
    monkeypatch.setattr(context, "ContextSeries", track)
    record = contracts.registration("feature", "btc_close_return")
    def view(grid, params, dependencies):
        source = next(iter(grid.sources[params["alias"]].values()))
        return contracts.FeatureValue(source.bars.close, source.valid)
    monkeypatch.setitem(contracts._REGISTRY["feature"], "btc_close_return",
                       replace(record, descriptor=replace(record.descriptor, evaluate=view)))
    gc.disable()
    try:
        with data.read_session(tmp_path/"pack") as session:
            outputs, facts = context.prepare(session, context.resolve_entries(session.inspect(verify=False), request), request)
        assert refs and all(ref() is None for ref in refs)
        for cache in outputs.values():
            for value in cache.values():
                assert value.values.base is None and value.valid.base is None
                assert not value.values.flags.writeable
    finally:
        gc.enable()
