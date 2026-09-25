"""Bounded metric ownership and one-admission integrity regressions."""
import copy
from pathlib import Path
import shutil
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabStudyError, analysis, study
from tools.pattern_lab.analysis import source, runner as analysis_runner, artifacts, report as analysis_report
from tools.pattern_lab.study import contracts, evidence, results, report
from tools.pattern_lab.examples import run_frozen_candidate as example
from . import _helpers as h, _metric_oracle as oracle
from ._context_helpers import context_fixture


def analysis_request():
    return {"schema_version": 2, "analysis_name": "coordinator regression", "model_instances": ["fh"], "pairwise": []}


def metric_fixture(monkeypatch, *, instruments=2, rows=4, columns=("net_return", "return_valid"), declarations=2):
    frames = {(i, str(g)): pd.DataFrame({"net_return": np.arange(rows, dtype=float) / 8 - .25,
                                        "return_valid": np.arange(rows) % 3 != 0,
                                        "text": ["value"] * rows}, index=np.arange(rows) + 10)
              for i in ("A", "B")[:instruments] for g in range(3)}
    calls, opened = [], []
    descriptors = {}
    declared = []
    for d in range(declarations):
        def compute(frame, d=d):
            calls.append(d)
            assert list(frame.columns) == list(columns)
            assert frame.index.equals(pd.RangeIndex(len(frame)))
            return None if frame.empty else float(frame.net_return.sum()) if "net_return" in columns else float(len(frame))
        descriptors[str(d)] = SimpleNamespace(required_columns=columns, compute=compute)
        doc = dict(declaration_id=str(d), metric_id=str(d), version="1", unit="fraction")
        declared.append(SimpleNamespace(**doc, as_json=lambda doc=doc: dict(doc)))
    monkeypatch.setattr(contracts, "metric", descriptors.__getitem__)
    class Reader:
        def __init__(self, saved, identifier): self.identifier = identifier; opened.append(identifier)
        def observations(self, *, case, **kwargs): return frames[self.identifier, case]
        def release(self): pass
    monkeypatch.setattr(results, "InstrumentReader", Reader)
    monkeypatch.setattr(oracle, "InstrumentReader", Reader)
    groups = [dict(timeframe_minutes=30, model_instance_id="fh", case_id=str(g), variant_id=str(g)) for g in range(3)]
    saved = SimpleNamespace(family={"models": [{"model_instance_id": "fh", "model_id": "fixed_horizon_path"}],
        "variants": [{"variant_id": str(g), "condition_id": "condition"} for g in range(3)], "groups": groups},
        completed_instruments=list(("A", "B")[:instruments]), case=lambda model, timeframe, case: case,
        jobs={i: {"stats": {"timeframes": {"30": {"conditions": {"condition": {"events_by_variant": {str(g): rows for g in range(3)}}}}}},
                  "files": [{"table": "emissions", "row_count": rows * 3}]} for i in ("A", "B")[:instruments]})
    return saved, declared, frames, descriptors, calls, opened


@pytest.mark.parametrize("budget", [64 * 1024 * 1024, 2500, 1])
def test_batched_metrics_match_oracle_and_owned_budget(monkeypatch, budget):
    saved, declarations, frames, _, calls, opened = metric_fixture(monkeypatch)
    expected = oracle.group_at_a_time(saved, declarations)
    calls.clear(); opened.clear()
    monkeypatch.setattr(results, "_METRIC_BUFFER_BUDGET", budget)
    observed = []
    collect = results._collect_metric_batch
    def checked(*args):
        value = collect(*args)
        if value is not None:
            parts = [p for group in value[0] for declaration in group for p in declaration]
            size = results._metric_part_bytes(parts)
            observed.append((len(args[1]), size))
            assert len(args[1]) == 1 or size <= budget
            for part in parts:
                for original in frames.values():
                    assert not np.shares_memory(part.net_return.to_numpy(), original.net_return.to_numpy())
                    assert not np.shares_memory(part.index.to_numpy(), original.index.to_numpy())
        return value
    monkeypatch.setattr(results, "_collect_metric_batch", checked)
    assert results.compute_metric_values(saved, declarations) == expected
    assert calls == [0, 1] * 3
    assert observed
    if budget == 1: assert all(n == 1 and size > budget for n, size in observed)
    if budget > 10000: assert opened == ["A", "B"]


@pytest.mark.parametrize("instruments", [1, 2])
def test_mutating_callback_has_declaration_owned_inputs(monkeypatch, instruments):
    saved, declarations, frames, descriptors, calls, _ = metric_fixture(monkeypatch, instruments=instruments)
    original = descriptors["0"].compute
    def mutate(frame):
        value = original(frame)
        frame.loc[:, "net_return"] = 999
        return value
    descriptors["0"].compute = mutate
    result = results.compute_metric_values(saved, declarations)
    assert all(result["values"][g]["value"] == result["values"][g + 1]["value"] for g in (0, 2, 4))
    assert all((frame.net_return < 999).all() for frame in frames.values())
    assert calls == [0, 1] * 3


@pytest.mark.parametrize("hint", ["zero", "under", "absent", "malformed", "absent_variant", "no_counts"])
def test_hint_is_not_an_artifact_rule_and_fallback_precedes_callbacks(monkeypatch, hint):
    saved, declarations, _, _, calls, _ = metric_fixture(monkeypatch, rows=20, declarations=1)
    expected = oracle.group_at_a_time(saved, declarations)
    calls.clear()
    for bundle in saved.jobs.values():
        events = bundle["stats"]["timeframes"]["30"]["conditions"]["condition"]["events_by_variant"]
        if hint in ("zero", "under", "malformed"):
            for key in events: events[key] = {"zero": 0, "under": 1, "malformed": True}[hint]
        if hint == "absent_variant": events.clear()
        if hint in ("absent", "no_counts"): bundle.pop("stats")
        if hint == "no_counts": bundle["files"][0]["row_count"] = None
    monkeypatch.setattr(results, "_METRIC_BUFFER_BUDGET", 3500)
    original = results._collect_metric_batch
    batches = []
    def collect(*args):
        value = original(*args)
        batches.append((len(args[1]), value is None, len(calls)))
        return value
    monkeypatch.setattr(results, "_collect_metric_batch", collect)
    assert results.compute_metric_values(saved, declarations) == expected
    assert calls == [0] * 3
    if hint in ("zero", "under", "absent_variant"):
        assert batches[0] == (3, True, 0)
        assert all(n == 1 and not failed for n, failed, _ in batches[1:])


@pytest.mark.parametrize("columns", [("text",), ("absent",)])
def test_unknown_or_missing_columns_keep_single_group_behavior(monkeypatch, columns):
    saved, declarations, _, _, calls, opened = metric_fixture(monkeypatch, columns=columns)
    expected = oracle.group_at_a_time(saved, declarations)
    calls.clear(); opened.clear()
    assert results.compute_metric_values(saved, declarations) == expected
    assert len(opened) == 6


def test_unexpected_object_dtype_falls_back_before_callbacks(monkeypatch):
    saved, declarations, frames, _, calls, _ = metric_fixture(monkeypatch)
    for frame in frames.values(): frame["net_return"] = frame.net_return.astype(object)
    expected = oracle.group_at_a_time(saved, declarations)
    calls.clear()
    collect = results._collect_metric_batch
    batches = []
    def observed(*args):
        value = collect(*args)
        batches.append((len(args[1]), value is None, len(calls)))
        return value
    monkeypatch.setattr(results, "_collect_metric_batch", observed)
    assert results.compute_metric_values(saved, declarations) == expected
    assert batches[0] == (3, True, 0)
    assert calls == [0, 1] * 3


def test_empty_declarations_or_groups_do_not_open_tables(monkeypatch):
    saved, declarations, _, _, _, opened = metric_fixture(monkeypatch)
    assert results.compute_metric_values(saved, [])['values'] == []
    saved.family["groups"] = []
    assert results.compute_metric_values(saved, declarations)['values'] == []
    assert not opened


@pytest.mark.parametrize("failure", ["nonfinite", "callback", "read"])
def test_isolated_errors_preserve_type_code_and_callback_prefix(monkeypatch, failure):
    saved, declarations, _, descriptors, calls, _ = metric_fixture(monkeypatch)
    def bad(frame):
        calls.append(0)
        if failure == "nonfinite": return float('inf')
        raise PatternLabDataError("callback failure", error_code="callback_probe")
    if failure == "read":
        def bad_read(self, **kwargs): raise PatternLabDataError("read failure", error_code="read_probe")
        monkeypatch.setattr(results.InstrumentReader, "observations", bad_read)
    else: descriptors["0"].compute = bad
    failures = []
    for compute in (oracle.group_at_a_time, results.compute_metric_values):
        calls.clear()
        with pytest.raises(PatternLabDataError) as caught: compute(saved, declarations)
        failures.append((type(caught.value), caught.value.error_code))
        assert calls in ([], [0])
    assert failures[0] == failures[1]


@pytest.fixture(scope="module")
def context_run(tmp_path_factory):
    root = tmp_path_factory.mktemp("coordinator-context")
    doc = context_fixture(root / "pack")
    study.run_study(request=doc, data_root=root / "pack", output_root=root / "study")
    return root / "study"


@pytest.mark.parametrize("change", ["counts", "identity", "schema", "missing_completion", "invalid_completion", "byte", "missing_file", "added_file", "resealed"])
def test_final_reverification_refuses_changes_without_decoding(context_run, tmp_path, monkeypatch, change):
    root = tmp_path / "source"
    shutil.copytree(context_run, root)
    original = source.reverify_source
    def changed(admitted, **kwargs):
        completion = evidence.read_json(root / evidence.COMPLETION_FILE)
        if change == "counts": completion["counts"]["planned"] += 1; completion["counts"]["completed"] += 1
        if change == "identity": completion["identities"]["specification_sha256"] = "a" * 64
        if change == "schema": completion["schema_version"] = 1
        if change == "invalid_completion": completion["schema_version"] = True
        evidence.write_json(root / evidence.COMPLETION_FILE, completion)
        if change == "missing_completion": (root / evidence.COMPLETION_FILE).unlink()
        target = root / evidence.REQUEST_FILE
        if change == "byte": target.write_bytes(target.read_bytes() + b" ")
        if change == "missing_file": target.unlink()
        if change == "added_file": (root / "jobs" / admitted.instruments[0] / "unexpected.json").write_text("{}")
        if change == "resealed":
            target.write_bytes(target.read_bytes() + b" ")
            completion["evidence_sha256"][evidence.REQUEST_FILE] = evidence.file_digest(target)
            completion["evidence_set_sha256"] = contracts.semantic_digest(completion["evidence_sha256"])
            evidence.write_json(root / evidence.COMPLETION_FILE, completion)
        def forbidden(*args, **kw): pytest.fail("final verification decoded or readmitted the source")
        with monkeypatch.context() as patch:
            patch.setattr(evidence, "read_table", forbidden)
            patch.setattr(results, "load_results", forbidden)
            return original(admitted, **kwargs)
    monkeypatch.setattr(source, "reverify_source", changed)
    with pytest.raises(PatternLabStudyError) as failure:
        analysis.run_analysis(request=analysis_request(), run_root=root, output_root=tmp_path / "analysis")
    assert failure.value.context["phase"] == "verify_source"
    assert isinstance(failure.value.__cause__, PatternLabDataError)
    assert not (tmp_path / "analysis/completion.json").exists()
    assert evidence.read_json(tmp_path / "analysis/status.json")["terminal_status"] == "failed"


def test_one_admission_with_relocated_and_derived_changes(context_run, tmp_path, monkeypatch):
    root = tmp_path / "relocated"
    shutil.copytree(context_run, root)
    count = []
    load = results.load_results
    def counted(*args, **kw): count.append(1); return load(*args, **kw)
    monkeypatch.setattr(results, "load_results", counted)
    original = source.reverify_source
    def changed(admitted, **kw):
        seal = evidence.read_json(root / evidence.COMPLETION_FILE)
        seal.update(run_root="/another/host/source", extra_note="allowed")
        evidence.write_json(root / evidence.COMPLETION_FILE, seal)
        (root / evidence.REPORT_FILE).write_text("regenerated excluded output")
        return original(admitted, **kw)
    monkeypatch.setattr(source, "reverify_source", changed)
    analysis.run_analysis(request=analysis_request(), run_root=root, output_root=tmp_path / "analysis")
    assert len(count) == 1
    analysis.load_analysis(tmp_path / "analysis")


@pytest.mark.parametrize("value", ["bad", "=empty", "module=", "module=missing", "module=file", "duplicate"])
def test_example_bad_roots_never_freeze(tmp_path, monkeypatch, value):
    (tmp_path / "file").touch()
    monkeypatch.chdir(tmp_path)
    args = ["example", "--study-root", "study", "--analysis-root", "analysis", "--candidate-output", "candidate.json",
            "--data-root", "pack", "--output-root", "out", "--start", "start", "--end", "end", "--warmup-start", "warmup"]
    if value == "duplicate": args += ["--extension-root", "module=.", "--extension-root", "module=."]
    else: args += ["--extension-root", value]
    monkeypatch.setattr("sys.argv", args)
    def forbidden(**kw): pytest.fail("invalid option reached freeze")
    monkeypatch.setattr(example, "freeze_candidate", forbidden)
    with pytest.raises(PatternLabDataError): example.main()
    assert not (tmp_path / "candidate.json").exists()


def test_contextual_report_labels_and_table_escaping():
    assert "disabled (not applied)" in report._fact_table({"maximum_stop_width_pct": None}, caption="settings", context="settings")
    rules = {"venue": "BYBIT", "quantity_unit": "ENA", "ct_val": None, "ct_mult": None,
             "minimum_notional": None, "enforce_minimum_notional": False}
    html = report._fact_table(rules, caption="rules", context="rules")
    assert html.count("not applicable") == 2
    assert "not published by venue (not enforced)" in html
    assert "unavailable" in report._fact_table(rules, caption="raw")
    for renderer in (report, analysis_report):
        html = renderer._rows([["<script>", "-1.23456"]], header=["<id>", "value"], caption="<caption>")
        assert html.startswith('<div class="table-scroll"><table>') and html.endswith('</table></div>')
        assert '<script>' not in html and '&lt;script&gt;' in html and '-1.23456' in html
        assert "max-width: 100%; overflow-x: auto" in renderer.STYLE
        assert "overflow-wrap: normal; word-break: normal" in renderer.STYLE


def test_initial_admission_checks_unselected_bracket_semantics(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from ._bracket_helpers import build
    from tools.pattern_lab.study import sequential
    root = tmp_path / "study"
    study.run_study(request=build(tmp_path / "pack"), data_root=tmp_path / "pack", output_root=root)
    path = root / "jobs/OKX_AAA-USDT-SWAP/sequential_path.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["balance"] += 1
    pq.write_table(pa.Table.from_pylist(rows, schema=sequential.SCHEMAS["path"]), path)
    bundle_path = path.parent / "bundle.json"
    bundle = evidence.read_json(bundle_path)
    for item in bundle["files"]:
        if item["name"] == path.name: item["sha256"] = evidence.file_digest(path)
    evidence.write_json(bundle_path, bundle)
    seal = evidence.read_json(root / evidence.COMPLETION_FILE)
    seal["evidence_sha256"] = {name:evidence.file_digest(root / name) for name in seal["evidence_sha256"]}
    seal["evidence_set_sha256"] = contracts.semantic_digest(seal["evidence_sha256"])
    evidence.write_json(root / evidence.COMPLETION_FILE, seal)
    evidence.verify_completion(root)  # physically coherent, semantically wrong
    with pytest.raises(PatternLabDataError):
        analysis.run_analysis(request=analysis_request(), run_root=root, output_root=tmp_path / "analysis")
    assert not (tmp_path / "analysis/completion.json").exists()


def test_real_metric_batches_match_original_group_loop(context_run, monkeypatch):
    saved = study.load_results(context_run)
    descriptor = SimpleNamespace(required_columns=("net_return", "return_valid"),
        compute=lambda frame: float(frame.loc[frame.return_valid, "net_return"].sum()))
    monkeypatch.setattr(contracts, "metric", lambda name: descriptor)
    doc = dict(declaration_id="probe", metric_id="probe", version="1", unit="fraction")
    declarations = [SimpleNamespace(**doc, as_json=lambda: doc)]
    expected = oracle.group_at_a_time(saved, declarations)
    for budget in (64 * 1024 * 1024, 1):
        monkeypatch.setattr(results, "_METRIC_BUFFER_BUDGET", budget)
        assert results.compute_metric_values(saved, declarations) == expected


def test_internal_summary_and_analysis_do_not_request_public_table_copies(context_run, tmp_path, monkeypatch):
    original = results.InstrumentReader.table
    def checked(reader, name):
        assert name not in ("conditions", "emissions", "episodes")
        return original(reader, name)
    monkeypatch.setattr(results.InstrumentReader, "table", checked)
    study.summarize_results(study.load_results(context_run))
    analysis.run_analysis(request=analysis_request(), run_root=context_run, output_root=tmp_path / "analysis")


@pytest.mark.parametrize("failure", ["exception", "nonfinite"])
def test_metric_failure_does_not_publish_metrics_or_completion(tmp_path, failure):
    extension = tmp_path / "extension"
    extension.mkdir()
    name = "t10_metric_failure_" + failure
    body = ('raise PatternLabDataError("metric callback failed", error_code="metric_probe")'
            if failure == "exception" else 'return float("inf")')
    (extension / (name + ".py")).write_text(
        'from tools.pattern_lab import PatternLabDataError\n'
        'from tools.pattern_lab.study import contracts\n'
        'def compute(frame):\n    ' + body + '\n'
        'def register(context):\n'
        '    context.register_metric(contracts.MetricDescriptor("' + name + '", "1", '
        'required_columns=("net_return", "return_valid"), unit="fraction", compute=compute))\n', encoding="utf-8")
    request = context_fixture(tmp_path / "pack")
    request["extensions"] = [{"module":name, "source_root":str(extension), "helpers":[]}]
    request["metrics"] = [{"id":"broken", "metric":name}]
    root = tmp_path / "run"
    with pytest.raises(PatternLabStudyError) as caught:
        study.run_study(request=request, data_root=tmp_path / "pack", output_root=root)
    assert isinstance(caught.value.__cause__, PatternLabDataError)
    if failure == "exception": assert caught.value.__cause__.error_code == "metric_probe"
    assert not (root / evidence.METRICS_FILE).exists()
    assert not (root / evidence.COMPLETION_FILE).exists()
    assert evidence.read_json(root / evidence.STATUS_FILE)["terminal_status"] == "failed"
