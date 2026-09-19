"""Trusted extensions, source integrity and the derived observation view.

Each case writes its own small module into task-owned temporary storage and
declares it explicitly, so the real digest-verified import path is exercised.
Module names are unique per case because a Python interpreter imports a module
once, which is exactly the condition the fresh-interpreter error describes.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import textwrap

import numpy as np
import pytest

from tools.pattern_lab import PatternLabDataError, PatternLabStudyError
from tools.pattern_lab import study as pack_study
from tools.pattern_lab.study import runner as study_runner

from ._helpers import (
    TWO_GREEN_EVERY_BAR,
    fixed_horizon_model,
    instrument_source,
    publish,
    study_group_ms,
    study_protocol,
    study_request,
    timeframe_bars,
)

TIMEFRAME = 30
GROUPS = 16
EXAMPLES_ROOT = Path(__file__).resolve().parents[2] / "tools" / "pattern_lab" / "examples"

HEADER = """
import numpy as np

from tools.pattern_lab.study import contracts
from tools.pattern_lab.study.contracts import (
    ConditionValue,
    FeatureDescriptor,
    FeatureRequest,
    FeatureValue,
    HypothesisDescriptor,
    MetricDescriptor,
    ModelCase,
    ModelDescriptor,
    ModelEvidence,
    OutcomeSpec,
)
"""


def write_module(root: Path, name: str, body: str, *, helpers=None) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{name}.py").write_text(HEADER + textwrap.dedent(body), encoding="utf-8")
    for helper_name, helper_body in (helpers or {}).items():
        (root / helper_name).write_text(textwrap.dedent(helper_body), encoding="utf-8")
    return root / f"{name}.py"


def rising(count: int = GROUPS):
    return tuple(
        (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index, 10.0 + index)
        for index in range(count)
    )


def build_pack(root: Path, contracts_map=None):
    sources = []
    for contract, specs in (contracts_map or {"AAA-USDT-SWAP": rising()}).items():
        stamps, values = timeframe_bars(TIMEFRAME, specs)
        sources.append(
            instrument_source(
                stamps, values, symbol=contract.split("-")[0], venue="TEST", contract=contract
            )
        )
    publish(root, sources)
    return root


def request_for(*, hypotheses=None, models=None, metrics=None, extensions=None):
    return study_request(
        protocol=study_protocol(
            first_ms=study_group_ms(0, TIMEFRAME),
            coverage_end_ms=study_group_ms(GROUPS + 8, TIMEFRAME),
        ),
        start_ms=study_group_ms(2, TIMEFRAME),
        end_ms=study_group_ms(12, TIMEFRAME),
        warmup_ms=study_group_ms(0, TIMEFRAME),
        timeframes=[TIMEFRAME],
        hypotheses=list(hypotheses if hypotheses is not None else [TWO_GREEN_EVERY_BAR]),
        models=list(models if models is not None else [fixed_horizon_model(TIMEFRAME, [30, 60])]),
        metrics=metrics,
        extensions=extensions,
    )


def declaration(root: Path, module: str, helpers=()):
    return {"module": module, "source_root": str(root), "helpers": list(helpers)}


# --------------------------------------------------------------------------
# the shipped example extension
# --------------------------------------------------------------------------

def test_the_shipped_example_extension_works_through_the_public_registry(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    document = request_for(
        hypotheses=[
            TWO_GREEN_EVERY_BAR,
            {
                "id": "sma_entry",
                "hypothesis": "example_close_above_sma",
                "parameters": {"period": 3},
                "occurrence": "state_entry",
            },
        ],
        models=[
            fixed_horizon_model(TIMEFRAME, [30, 60]),
            {"id": "gap", "model": "example_next_open_gap", "settings": {}},
        ],
        metrics=[{"id": "positive", "metric": "example_positive_net_share"}],
        extensions=[declaration(EXAMPLES_ROOT, "custom_extension")],
    )
    result = pack_study.run_study(request=document, data_root=pack, output_root=run_root)
    assert result["counts"]["completed"] == 1

    results = pack_study.load_results(run_root)
    summary = pack_study.summarize_results(results)
    axis_free = [group for group in summary["groups"] if group["case_id"] == "tf30m.axis_free"]
    # The axis-free model resolves exactly one case per timeframe and variant.
    assert len(axis_free) == 2
    assert [outcome["name"] for outcome in axis_free[0]["outcomes"]] == ["open_gap"]
    assert axis_free[0]["case_parameters"] == {}

    # A metric whose required column is not part of that view is explicitly
    # unavailable rather than guessed.
    unavailable = axis_free[0]["metrics"][0]
    assert unavailable["availability"] == "missing_inputs"
    assert unavailable["missing_columns"] == ["net_return"]
    fixed = next(
        group for group in summary["groups"] if group["case_id"] == "tf30m.h60m.long"
    )
    assert fixed["metrics"][0]["availability"] == "available"


def test_the_example_script_reports_a_missing_pack_without_a_traceback(tmp_path, capsys):
    from tools.pattern_lab.examples import run_example_study

    code = run_example_study.main(
        [
            "--data-root", str(tmp_path / "absent"),
            "--output-root", str(tmp_path / "run"),
            "--instrument", "TEST_AAA-USDT-SWAP",
            "--start", "2025-07-01T00:00:00Z",
            "--end", "2025-08-01T00:00:00Z",
            "--warmup-start", "2025-06-01T00:00:00Z",
        ]
    )
    assert code == 2
    assert "does not exist" in capsys.readouterr().err


# --------------------------------------------------------------------------
# malformed extensions
# --------------------------------------------------------------------------

BAD_SHAPE = """
def _evaluate(series, parameters, features):
    rows = series.row_count
    return ConditionValue(
        value=np.zeros(rows - 1, dtype=bool), valid=np.zeros(rows - 1, dtype=bool)
    )


DESCRIPTOR = HypothesisDescriptor(
    hypothesis_id="ext_shape_condition", version="1", evaluate=_evaluate
)


def register(context):
    context.register_hypothesis(DESCRIPTOR)
"""

BAD_DTYPE = """
def _evaluate(series, parameters, features):
    rows = series.row_count
    return ConditionValue(
        value=np.zeros(rows, dtype=np.float64), valid=np.ones(rows, dtype=bool)
    )


DESCRIPTOR = HypothesisDescriptor(
    hypothesis_id="ext_dtype_condition", version="1", evaluate=_evaluate
)


def register(context):
    context.register_hypothesis(DESCRIPTOR)
"""

PANEL_SCOPE = """
def _evaluate(series, parameters, features):
    rows = series.row_count
    return ConditionValue(value=np.zeros(rows, dtype=bool), valid=np.ones(rows, dtype=bool))


DESCRIPTOR = HypothesisDescriptor(
    hypothesis_id="ext_panel_condition", version="1", evaluate=_evaluate, scope="panel"
)


def register(context):
    context.register_hypothesis(DESCRIPTOR)
"""

BUILTIN_CLASH = """
def _evaluate(series, parameters, features):
    rows = series.row_count
    return ConditionValue(value=np.zeros(rows, dtype=bool), valid=np.ones(rows, dtype=bool))


DESCRIPTOR = HypothesisDescriptor(
    hypothesis_id="two_green_rising_quote_volume", version="9", evaluate=_evaluate
)


def register(context):
    context.register_hypothesis(DESCRIPTOR)
"""

UNDECLARED_CASE = """
def _validate(settings, timeframes):
    contracts.closed_keys(settings, (), "settings")
    return {}


def _cases(settings, timeframe_minutes):
    return (
        ModelCase(
            case_id="only_case",
            timeframe_minutes=int(timeframe_minutes),
            parameters={},
            outcomes=(OutcomeSpec("score", "fraction"),),
        ),
    )


def _evaluate(series, settings, anchors):
    count = int(anchors.rows.size)
    return ModelEvidence(
        kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
        rows={
            "case_id": np.full(count, "undeclared_case", dtype=object),
            "anchor_open_ms": anchors.open_ms.astype(np.int64),
            "score": np.zeros(count),
            "score__reason": np.full(count, "available", dtype=object),
        },
    )


DESCRIPTOR = ModelDescriptor(
    model_id="ext_undeclared_case",
    version="1",
    validate_settings=_validate,
    resolve_cases=_cases,
    evaluate=_evaluate,
    evidence_kind=contracts.CUSTOM_CASE_EVIDENCE_KIND,
)


def register(context):
    context.register_model(DESCRIPTOR)
"""

MISSING_COLUMN = UNDECLARED_CASE.replace(
    'model_id="ext_undeclared_case"', 'model_id="ext_missing_column"'
).replace(
    '            "score": np.zeros(count),\n            "score__reason": np.full(count, "available", dtype=object),\n',
    "",
).replace('np.full(count, "undeclared_case", dtype=object)', 'np.full(count, "only_case", dtype=object)')

RESERVED_OUTCOME = UNDECLARED_CASE.replace(
    'model_id="ext_undeclared_case"', 'model_id="ext_reserved_outcome"'
).replace('OutcomeSpec("score", "fraction")', 'OutcomeSpec("case_id", "fraction")')


def run_with_module(tmp_path, name, body, *, hypothesis=None, model=None):
    pack = build_pack(tmp_path / "pack")
    write_module(tmp_path / "ext", name, body)
    hypotheses = [TWO_GREEN_EVERY_BAR] if hypothesis is None else [hypothesis]
    models = (
        [fixed_horizon_model(TIMEFRAME, [30])] if model is None else [model]
    )
    document = request_for(
        hypotheses=hypotheses,
        models=models,
        extensions=[declaration(tmp_path / "ext", name)],
    )
    return pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )


def test_a_condition_with_the_wrong_shape_is_rejected(tmp_path):
    with pytest.raises(PatternLabStudyError) as failure:
        run_with_module(
            tmp_path, "ext_shape", BAD_SHAPE,
            hypothesis={
                "id": "bad", "hypothesis": "ext_shape_condition", "parameters": {},
                "occurrence": "every_qualifying_bar",
            },
        )
    assert "expected aligned" in str(failure.value.__cause__)


def test_a_condition_with_the_wrong_dtype_is_rejected(tmp_path):
    with pytest.raises(PatternLabStudyError) as failure:
        run_with_module(
            tmp_path, "ext_dtype", BAD_DTYPE,
            hypothesis={
                "id": "bad", "hypothesis": "ext_dtype_condition", "parameters": {},
                "occurrence": "every_qualifying_bar",
            },
        )
    assert "must both be boolean" in str(failure.value.__cause__)


def test_unsupported_panel_scope_fails_before_any_work(tmp_path):
    build_pack(tmp_path / "pack")
    write_module(tmp_path / "ext", "ext_panel", PANEL_SCOPE)
    with pytest.raises(PatternLabDataError, match="scope 'panel' is not supported"):
        pack_study.normalize_request(
            request_for(extensions=[declaration(tmp_path / "ext", "ext_panel")]),
            source="test",
            base=None,
        )
    assert not (tmp_path / "run").exists()


def test_a_custom_registration_never_silently_replaces_a_built_in(tmp_path):
    write_module(tmp_path / "ext", "ext_clash", BUILTIN_CLASH)
    with pytest.raises(PatternLabDataError, match="already registered as a built-in"):
        pack_study.normalize_request(
            request_for(extensions=[declaration(tmp_path / "ext", "ext_clash")]),
            source="test",
            base=None,
        )


def test_an_undeclared_case_a_missing_column_and_a_reserved_outcome_are_rejected(tmp_path):
    with pytest.raises(PatternLabStudyError) as failure:
        run_with_module(
            tmp_path / "a", "ext_case", UNDECLARED_CASE,
            model={"id": "custom", "model": "ext_undeclared_case", "settings": {}},
        )
    assert "undeclared case IDs" in str(failure.value.__cause__)

    with pytest.raises(PatternLabStudyError) as failure:
        run_with_module(
            tmp_path / "b", "ext_column", MISSING_COLUMN,
            model={"id": "custom", "model": "ext_missing_column", "settings": {}},
        )
    assert "missing columns" in str(failure.value.__cause__)

    build_pack(tmp_path / "c" / "pack")
    write_module(tmp_path / "c" / "ext", "ext_reserved", RESERVED_OUTCOME)
    with pytest.raises(PatternLabDataError, match="collides with the reserved evidence field"):
        pack_study.normalize_request(
            request_for(
                models=[{"id": "custom", "model": "ext_reserved_outcome", "settings": {}}],
                extensions=[declaration(tmp_path / "c" / "ext", "ext_reserved")],
            ),
            source="test",
            base=None,
        )


# --------------------------------------------------------------------------
# source integrity
# --------------------------------------------------------------------------

SIMPLE_METRIC = """
from helper_math import share_above_zero


def _compute(frame):
    return share_above_zero(frame["net_return"].to_numpy(dtype=np.float64))


DESCRIPTOR = MetricDescriptor(
    metric_id="ext_share_metric",
    version="1",
    required_columns=("net_return",),
    unit="fraction",
    compute=_compute,
)


def register(context):
    context.register_metric(DESCRIPTOR)
"""

HELPER_MATH = """
import numpy as np


def share_above_zero(values):
    values = values[np.isfinite(values)]
    if values.size == 0:
        return None
    return float(np.count_nonzero(values > 0.0) / values.size)
"""


def test_a_declared_helper_is_hashed_and_a_mid_run_mutation_is_detected(tmp_path):
    pack = build_pack(tmp_path / "pack")
    root = tmp_path / "ext"
    write_module(root, "ext_integrity", SIMPLE_METRIC, helpers={"helper_math.py": HELPER_MATH})
    document = request_for(
        metrics=[{"id": "share", "metric": "ext_share_metric"}],
        extensions=[declaration(root, "ext_integrity", ["helper_math.py"])],
    )
    result = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )
    source = json.loads((Path(result["run_root"]) / "spec" / "source.json").read_text())
    recorded = {item["path"] for item in source["extensions"][0]["files"]}
    assert recorded == {"ext_integrity.py", "helper_math.py"}

    # A helper edited while the run is in flight is detected before the result
    # of that job is accepted.
    original = study_runner.run_instrument_job

    def mutating_job(payload):
        (root / "helper_math.py").write_text(HELPER_MATH + "\n# edited\n", encoding="utf-8")
        return original(payload)

    study_runner.run_instrument_job = mutating_job
    try:
        with pytest.raises(PatternLabStudyError) as failure:
            pack_study.run_study(
                request=document, data_root=pack, output_root=tmp_path / "run-2"
            )
    finally:
        study_runner.run_instrument_job = original
        (root / "helper_math.py").write_text(HELPER_MATH, encoding="utf-8")
    assert failure.value.error_code == "source_changed"


def test_an_edited_module_cannot_be_reused_in_the_same_interpreter(tmp_path):
    pack = build_pack(tmp_path / "pack")
    root = tmp_path / "ext"
    write_module(root, "ext_reimport", SIMPLE_METRIC, helpers={"helper_math.py": HELPER_MATH})
    document = request_for(
        metrics=[{"id": "share", "metric": "ext_share_metric"}],
        extensions=[declaration(root, "ext_reimport", ["helper_math.py"])],
    )
    pack_study.run_study(request=document, data_root=pack, output_root=tmp_path / "run")

    (root / "ext_reimport.py").write_text(
        (root / "ext_reimport.py").read_text(encoding="utf-8") + "\n# changed\n", encoding="utf-8"
    )
    with pytest.raises(PatternLabDataError) as failure:
        pack_study.run_study(request=document, data_root=pack, output_root=tmp_path / "run-2")
    assert failure.value.error_code == "source_changed"
    assert "fresh interpreter" in str(failure.value)


def test_a_snapshot_is_inert_provenance_and_is_never_imported(tmp_path):
    pack = build_pack(tmp_path / "pack")
    root = tmp_path / "ext"
    write_module(root, "ext_snapshot", SIMPLE_METRIC, helpers={"helper_math.py": HELPER_MATH})
    document = request_for(
        metrics=[{"id": "share", "metric": "ext_share_metric"}],
        extensions=[declaration(root, "ext_snapshot", ["helper_math.py"])],
    )
    result = pack_study.run_study(
        request=document, data_root=pack, output_root=tmp_path / "run"
    )
    run_root = Path(result["run_root"])
    snapshots = sorted(path.name for path in (run_root / "spec" / "snapshots").iterdir())
    assert snapshots == ["ext_snapshot__ext_snapshot.py", "ext_snapshot__helper_math.py"]
    assert not any(name.startswith("ext_snapshot__") for name in sys.modules)

    # Regeneration reuses the recorded metric values without importing anything.
    for path in root.iterdir():
        path.unlink()
    before = set(sys.modules)
    pack_study.regenerate_report(run_root)
    assert set(sys.modules) - before == set()
    summary = json.loads((run_root / "derived" / "summary.json").read_text())
    assert summary["metrics"]["values"][0]["metric_id"] == "ext_share_metric"


def test_a_missing_declared_module_or_helper_is_named(tmp_path):
    with pytest.raises(PatternLabDataError, match="is not an existing directory"):
        pack_study.normalize_request(
            request_for(extensions=[declaration(tmp_path / "absent-root", "ext_none")]),
            source="test",
            base=None,
        )
    root = tmp_path / "ext"
    write_module(root, "ext_helper_missing", SIMPLE_METRIC)
    with pytest.raises(PatternLabDataError, match="declared helper 'helper_math.py' does not exist"):
        pack_study.normalize_request(
            request_for(
                extensions=[declaration(root, "ext_helper_missing", ["helper_math.py"])]
            ),
            source="test",
            base=None,
        )
    with pytest.raises(PatternLabDataError, match="not a plain module name"):
        pack_study.normalize_request(
            request_for(extensions=[declaration(root, "package.module")]),
            source="test",
            base=None,
        )


# --------------------------------------------------------------------------
# raw and derived joins
# --------------------------------------------------------------------------

def test_the_observation_view_joins_emissions_to_the_stored_primitives(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    results = pack_study.load_results(run_root)

    emissions = results.table("TEST_AAA-USDT-SWAP", "emissions")
    primitives = results.table("TEST_AAA-USDT-SWAP", "primitives")
    events = results.observations(
        "TEST_AAA-USDT-SWAP",
        model_instance_id="fh",
        timeframe_minutes=TIMEFRAME,
        case_id="tf30m.h60m.short",
        variant_id="two_green_every",
    )
    assert len(events) == len(emissions)
    assert set(events["event_id"]) == set(emissions["event_id"])

    anchors = results.observations(
        "TEST_AAA-USDT-SWAP",
        model_instance_id="fh",
        timeframe_minutes=TIMEFRAME,
        case_id="tf30m.h60m.short",
    )
    assert len(anchors) == len(primitives.loc[primitives["horizon_minutes"] == 60])

    # The derived short values follow from the raw primitives and the case.
    raw = primitives.loc[
        (primitives["horizon_minutes"] == 60) & primitives["return_valid"]
    ].iloc[0]
    derived = anchors.set_index("anchor_open_ms").loc[int(raw["anchor_open_ms"])]
    ratio = float(raw["exit_price"]) / float(raw["entry_price"])
    assert derived["gross_return"] == pytest.approx(-(ratio - 1.0))
    assert derived["net_return"] == pytest.approx(-(ratio - 1.0) - 0.0005 * (1.0 + ratio))
    assert derived["mfe"] == pytest.approx(
        max(0.0, 1.0 - float(raw["path_low"]) / float(raw["entry_price"]))
    )


def test_a_table_a_job_did_not_save_is_reported_rather_than_guessed(tmp_path):
    pack = build_pack(tmp_path / "pack")
    run_root = tmp_path / "run"
    pack_study.run_study(request=request_for(), data_root=pack, output_root=run_root)
    results = pack_study.load_results(run_root)
    with pytest.raises(PatternLabDataError, match="requires a new explicit study"):
        results.table("TEST_AAA-USDT-SWAP", "custom__absent")
