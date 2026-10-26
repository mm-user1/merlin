"""Admission in Fast selection, both packing routes and independent thread counts."""
from copy import deepcopy
from dataclasses import replace
import json

import numba
import numpy as np
import pytest

from core.data_input import prepare_input
from core.engine_v2.admission import AdmissionRules
from core.engine_v2.compiled_kernel import pack_compiled_config_arrays_from_rows, _pack_config_arrays
from core.engine_v2.profile import parse_execution_profile
from core.grid_v2 import GridV2Settings, GridV2StrategyHooks, build_grid_v2_plan, execute_grid_v2_candidates
from tests.fixtures.admission_strategy import strategy
from tests.fixtures.admission_strategy.bootstrap import publish, grid_payload


@pytest.mark.parametrize("workers", [1, 2])
def test_fast_admission_packers_and_threads(tmp_path, workers):
    assert numba.config.NUMBA_NUM_THREADS >= workers, "This focused gate needs the specified actual Numba thread capacity."
    prepared = prepare_input(publish(tmp_path / "pack", rows=600), date_filter=False, admission_params={})
    config = strategy.load_config()
    params = grid_payload()["fixed_params"]
    hooks = GridV2StrategyHooks.from_strategy(strategy)
    settings = GridV2Settings(enabled_axes=("signalEvery",), top_n=2, compiled_workers=workers,
        admission_rules=prepared.admission_rules)
    plan = build_grid_v2_plan(config, settings, params)
    previous = numba.get_num_threads()
    try:
        numba.set_num_threads(workers)
        table = execute_grid_v2_candidates(plan, prepared.frame, 0, hooks)
        mapping = execute_grid_v2_candidates(replace(plan, settings=replace(settings, compiled_config_packing="mapping")), prepared.frame, 0, hooks)
        reference = execute_grid_v2_candidates(replace(plan, settings=replace(settings, prefer_compiled=False)), prepared.frame, 0, hooks)
    finally:
        numba.set_num_threads(previous)
    assert table.metadata["compiled_workers"] == workers
    assert table.metadata["compiled_config_packing"] == "table"
    assert mapping.metadata["compiled_config_packing"] == "mapping"
    for a,b,c in zip(table.rows, mapping.rows, reference.rows):
        assert a.total_trades == b.total_trades == c.total_trades
        assert a.net_profit_pct == pytest.approx(c.net_profit_pct, rel=1e-12)
        assert a.net_profit_pct == b.net_profit_pct
        assert a.guardrail_summary == b.guardrail_summary
        assert a.guardrail_summary == {key: c.guardrail_summary[key] for key in a.guardrail_summary}
    assert all(s.entry_diagnostics["maxEntryLeverageUsed"] > 0 for s in table.selected)
    capped = build_grid_v2_plan(config, settings, {**params, "maxEntryLeverage": .001})
    refused = execute_grid_v2_candidates(capped, prepared.frame, 0, hooks)
    assert capped.plan_fingerprint != plan.plan_fingerprint
    assert all(row.total_trades == 0 and row.net_profit_pct == 0 for row in refused.rows)
    assert all(s.entry_diagnostics["leverageCapSkippedEntries"] > 0 for s in refused.selected)
    assert all(row.net_profit_pct > 0 for row in table.rows)
    sampled_settings = replace(settings, planning_policy="sampled", requested_budget=1)
    sampled = build_grid_v2_plan(config, sampled_settings, params)
    sample = execute_grid_v2_candidates(sampled, prepared.frame, 0, hooks)
    assert len(sample.rows) == 1 and sample.rows[0].status == "ok"
    other_rules = AdmissionRules.from_entry(strategy.rule_entry(step="0.2"))
    other = build_grid_v2_plan(config, replace(settings, admission_rules=other_rules), params)
    assert other.plan_fingerprint != plan.plan_fingerprint
    # Separate legacy plan in the same process cannot inherit the admission cap.
    legacy = deepcopy(config)
    legacy["execution"].pop("entryAdmission")
    legacy["parameters"].pop("maxEntryLeverage")
    legacy["parameters"]["contractSize"] = dict(type="float", role="execution", default=.1)
    legacy["parameters"]["stopMaxPct"] = dict(type="float", role="execution", default=100.)
    legacy_params = {k:v for k,v in params.items() if k != "maxEntryLeverage"}
    old = build_grid_v2_plan(legacy, replace(settings, admission_rules=None), legacy_params)
    old_result = execute_grid_v2_candidates(old, prepared.frame, 0, hooks)
    assert all(s.entry_diagnostics is None for s in old_result.selected)


def test_typed_packer_fixed_cap_guards_and_numerical_error(tmp_path):
    config = strategy.load_config()
    profile = parse_execution_profile(config)
    params = grid_payload()["fixed_params"]
    rules = AdmissionRules.from_entry(strategy.rule_entry())
    get_value = lambda i,name,default: params.get(name, default)
    get_modes = lambda i: profile.modes
    with pytest.raises(ValueError, match="rules"):
        pack_compiled_config_arrays_from_rows(row_count=1, get_value=get_value, get_modes=get_modes, trade_start_idx=0, profile=profile)
    mapped = _pack_config_arrays(profile, [params], 0, rules)
    typed = pack_compiled_config_arrays_from_rows(row_count=1, get_value=get_value, get_modes=get_modes, trade_start_idx=0, profile=profile, admission_rules=rules)
    for key in mapped:
        np.testing.assert_equal(mapped[key],typed[key])
    for values, settings in [({**params,"contractSize":1},GridV2Settings(admission_rules=rules)),
        ({**params,"stopMaxPct":10},GridV2Settings(admission_rules=rules)),
        (params,GridV2Settings(admission_rules=rules,enabled_axes=("maxEntryLeverage",))),
        ({**params,"maxEntryLeverage_options":[2,8]},GridV2Settings(admission_rules=rules))]:
        with pytest.raises(ValueError):
            build_grid_v2_plan(config,settings,values)
    prepared=prepare_input(publish(tmp_path/"pack", rows=100),date_filter=False,admission_params={})
    plan=build_grid_v2_plan(config,GridV2Settings(admission_rules=rules,enabled_axes=("signalEvery",)),{**params,"riskPerTrade":1e308})
    result=execute_grid_v2_candidates(plan,prepared.frame,0,GridV2StrategyHooks.from_strategy(strategy))
    assert all(row.status == "error" for row in result.rows)
    assert not result.selected
