"""Validated saved-family adapters. No execution, availability reranking or curve retention."""
from copy import deepcopy
from dataclasses import asdict
import hashlib
from pathlib import Path
import pandas as pd

from .ranking import require, stamp
from ..analysis.dataset import AnalysisError
from ..analysis.research_inputs import _read
from ..analysis.candidate_policy import validate_policy_metrics
from ..analysis.rules import evaluate_rule, select_candidates, star_neighbours, RULE_REQUIREMENTS
from ..fixed import storage
from ..fixed.config import RUNTIME
from ..filters import recorded_roots
from ..report_support import finite, number, saved_roots, source_facts


def instrument_join(pin, ticker, tf):
    source, instrument = pin["source"], pin["instrument"]
    venue, contract = ticker.split("_", 1)
    require(source["type"] == "parquet" and source["instrument_id"] == ticker and source["timeframe_minutes"] == tf and
        instrument["instrument_id"] == ticker and instrument["venue"] == venue and instrument["contract"] == contract,
        "ranking/outcome canonical instrument or trading TF mismatch.")


def fixed_outcomes(ranking, inputs, parameter_sets, allow_partial):
    m = ranking.metadata
    require(m["request"]["effective"]["calendar"]["mode"] == "fixed", "Fixed outcomes require fixed ranking mode.")
    roster = sorted(m["universe"]["document"]["tickers"])
    tf = m["request"]["effective"]["calendar"]["tradingTimeframeMinutes"]
    step = pd.Timedelta(minutes=tf)
    periods = m["calendar"]["periods"]
    return collect_fixed(roster, tf, periods, inputs, parameter_sets, allow_partial)


def collect_fixed(roster, tf, periods, inputs, parameter_sets, allow_partial):
    step = pd.Timedelta(minutes=tf)
    expected = {(ticker, p["start"], p["end"]): p["id"] for p in periods for ticker in roster}
    sources, outcomes, roots, identities = {}, {}, [], set()
    require(not set(parameter_sets)-{label for label, _ in inputs}, "unknown parameter-set label.")
    for label, path in inputs:
        path = Path(path).absolute()
        captured = {}
        def capture(account, result, monthly):
            require("net_profit_pct" in result["metrics"], "required Fixed net_profit_pct metric unavailable.")
            captured[account["account_id"]] = deepcopy(result["metrics"])
        manifest = storage.open_dataset(path, allow_partial=allow_partial, on_account=capture)
        require(manifest["contract_sha256"] not in identities, "duplicate Fixed input identity.")
        identities.add(manifest["contract_sha256"])
        contract = manifest["contract"]
        names = [p["name"] for p in contract["resolved"]["parameter_sets"]]
        selected = parameter_sets.get(label)
        require(selected in names if selected is not None else len(names) == 1, "Fixed parameter set must be explicit and unambiguous.")
        selected = selected or names[0]
        index, unused, params = {}, [], None
        for a in contract["accounts"]:
            bounds = a["input_pin"]["bounds"]
            key = (a["instrument_id"], stamp(bounds["evaluation_start"]).isoformat(), stamp(bounds["end"]).isoformat())
            if a["parameter_set"] != selected or a["timeframe_minutes"] != tf or key not in expected:
                unused.append(a["account_id"])
                continue
            require(key not in index, "duplicate Fixed semantic cell.")
            instrument_join(a["input_pin"], a["instrument_id"], tf)
            require(stamp(bounds["start"]) == stamp(key[1]) and stamp(bounds["evaluation_end"])+step == stamp(key[2]), "Fixed last-bar/end semantics mismatch.")
            fixed = {k:v for k,v in a["params"].items() if k not in RUNTIME}
            require(params is None or fixed == params, "non-runtime parameters/economics vary within label.")
            params = fixed
            index[key] = a
        require(set(index) == set(expected), "Fixed input lacks the full exact ticker/period matrix; continuous accounts cannot be sliced.")
        label_rows = {}
        for key, a in index.items():
            result = captured.get(a["account_id"])
            status = "completed" if result is not None else "failed" if manifest["failure"] and manifest["failure"]["account_id"] == a["account_id"] else "pending"
            metrics = result if result is not None else {}
            value = number(metrics.get("net_profit_pct"))
            label_rows[expected[key], key[0]] = dict(status=status if value is not None else "unknown", reason=None if value is not None else
                "nonfinite_net_profit_pct" if result else status, value=value, account_id=a["account_id"], semantic_key=None,
                candidate_id=None, is_net_profit_pct=None, is_total_trades=None, metrics=deepcopy(metrics))
        manifest_path = path/("manifest.json" if manifest["complete"] else "manifest.partial.json")
        sources[label] = dict(path=str(path), schema_version=manifest["schema_version"], manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            manifest=manifest, parameter_set=selected, resolved_params=params, unused_account_ids=unused)
        roots.extend([path, *recorded_roots(contract["resolved"].get("entry_filters", [])),
            *(a["provenance"]["pack_root"] for a in contract["accounts"])])
        outcomes[label] = label_rows
    return outcomes, sources, roots, []


def candidate_outcomes(ranking, inputs, rule, policy):
    m = ranking.metadata
    require(m["request"]["effective"]["calendar"]["mode"] == "is_oos", "IS/OOS outcomes require is_oos ranking mode.")
    require((rule is None) != (policy is None), "supply exactly one candidate rule or policy.")
    roster = sorted(m["universe"]["document"]["tickers"])
    tf = m["request"]["effective"]["calendar"]["tradingTimeframeMinutes"]
    step = pd.Timedelta(minutes=tf)
    windows = [w for p in m["calendar"]["periods"] for w in p["windows"]]
    return collect_candidates(roster, tf, windows, inputs, rule, policy)


def collect_candidates(roster, tf, windows, inputs, rule, policy, *, history_scopes=None, history_ids=()):
    require((rule is None) != (policy is None), "supply exactly one candidate rule or policy.")
    if policy is not None:
        policy.verify()
    step = pd.Timedelta(minutes=tf)
    sources, roots, states, identities = {}, [], {}, set()
    for item in inputs:
        d, scope, label = item.dataset, item.scope, item.label
        roles = {"evaluation": scope}
        if history_scopes is not None:
            roles["history"] = history_scopes[label]
            require(roles["history"].ticker_cell == scope.ticker_cell, "history/evaluation ticker-cell mismatch.")
        for role in roles.values():
            if history_scopes is not None:
                require(not role.missing_window_ids, "partial scope is not authorized for history evaluation.")
                resolved = d.resolve_scope(role.name, unlock=bool(role.unlock_evidence),
                    policy_path=role.unlock_evidence["policy_ref"] if role.unlock_evidence else None)
                require(role == resolved, "history evaluation requires an unchanged resolved scope.")
            require(set(roster) <= set(role.tickers), "ranking roster is outside authorized scope.")
        require("net_profit_pct" in d.metric_index, "required candidate net_profit_pct metric unavailable.")
        require(d.schema_version in ("strategy_lab_dataset_v3", "strategy_lab_dataset_v4") and d.status == "complete", "X requires complete saved Parquet v3/v4 inputs.")
        require(d.manifest_sha256 not in identities, "duplicate candidate dataset identity.")
        identities.add(d.manifest_sha256)
        require(set(roster) <= set(scope.tickers), "ranking roster is outside authorized scope.")
        require(d.run_spec["generation"]["market_data"]["timeframe_minutes"] == tf, "trading TF mismatch.")
        if policy is not None:
            validate_policy_metrics(policy, d.metric_index, label)
        else:
            require(rule in RULE_REQUIREMENTS and rule in d.contract.rule_registry["selectable_rules"], "unknown or undeclared candidate rule.")
            require(set(RULE_REQUIREMENTS[rule]) <= set(d.metric_index), "candidate rule metric unavailable.")
        mapped = {}
        owners = {}
        for w in windows:
            owner = roles["history"] if history_scopes is not None and w["id"] in history_ids else scope
            matches = [s for s in owner.windows if stamp(s.oos_start) == stamp(w["start"]) and stamp(s.oos_end)+step == stamp(w["end"])
                and stamp(s.is_start) == stamp(w["isStart"]) and stamp(s.is_end)+step == stamp(w["start"])]
            require(len(matches) == 1, "ranking window is absent, ambiguous or outside authorized exact IS/OOS scope.")
            mapped[w["id"]] = matches[0]
            owners[w["id"]] = owner
        _, _, bindings = _read(d.root/"input_bindings.json")
        by_id = {b["instrument_id"]:b for b in bindings["instruments"]}
        require(set(roster) <= set(by_id), "required instrument bindings unavailable.")
        for ticker in roster:
            instrument_join(by_id[ticker]["input_pin"], ticker, tf)
        sources[label] = dict(**source_facts(d, scope), bindings=bindings,
            unused_tickers=sorted(set(scope.tickers)-set(roster)),
            unused_window_ids=sorted({w.window_id for w in scope.windows}-{w.window_id for w in mapped.values()}))
        if history_scopes is not None:
            sources[label]["candidate_semantics"] = dict(rule_registry=deepcopy(d.contract.rule_registry),
                params=[deepcopy(p) for p in d.geometry.params], axis_names=list(d.geometry.global_axis_names),
                axis_codes=d.geometry.axis_codes.tolist(), active_masks=d.geometry.active_masks.tolist())
            sources[label]["scope_roles"] = {name: dict(scope=asdict(role),
                unused_tickers=sorted(set(role.tickers)-set(roster)),
                unused_window_ids=sorted({w.window_id for w in role.windows}-
                    {mapped[i].window_id for i in mapped if (i in history_ids) == (name == "history")}))
                for name, role in roles.items()}
            for role in roles.values():
                if role.unlock_evidence:
                    roots.append(role.unlock_evidence["policy_ref"])
        roots.extend(saved_roots(d))
        if scope.unlock_evidence:
            roots.append(scope.unlock_evidence["policy_ref"])
        states[label] = dict(item=item, windows=mapped, owners=owners, neighbours=star_neighbours(d.geometry), access_start=len(d.access_log))
    outcomes, trace = {i.label:{} for i in inputs}, []
    for w in windows:
        frozen = {}
        for label, state in states.items():
            item = state["item"]; d = item.dataset; window = state["windows"][w["id"]]
            scope = d.subset_scope(state["owners"][w["id"]], tickers=roster, window_ids=[window.window_id])
            views = d.load_is_window(scope, window)
            choices = {}
            for ticker, view in views.items():
                result = policy.evaluate(view, d.geometry) if policy is not None else evaluate_rule(rule, view, d.geometry,
                    float(d.contract.rule_registry["minimum_completed_trades"]), neighbours=state["neighbours"])
                selected = select_candidates(result, view.metrics["net_profit_pct"], d.geometry, maximum=1)
                index = selected.row_indices[0] if selected.row_indices else None
                choices[ticker] = dict(index=index, semantic_key=selected.semantic_keys[0] if index is not None else None,
                    candidate_id=int(selected.candidate_ids[0]) if index is not None else None,
                    is_net_profit_pct=number(view.metrics["net_profit_pct"][index]) if index is not None else None,
                    is_total_trades=number(view.metrics["total_trades"][index]) if index is not None and "total_trades" in view.metrics else None)
            frozen[label] = (scope, window, choices)
            trace.append(dict(event="is_frozen", label=label, block_id=w["id"], tickers=len(choices)))
            del views, view, result
        for label, (scope, window, choices) in frozen.items():
            d = states[label]["item"].dataset
            trace.append(dict(event="oos_reveal", label=label, block_id=w["id"]))
            arrays = d.load_oos_window(scope, window)
            for ticker, choice in choices.items():
                index = choice.pop("index")
                metrics = {name:number(arrays[ticker][index, j]) for name,j in d.metric_index.items()} if index is not None else {}
                value = metrics.get("net_profit_pct") if index is not None else 0.0
                outcomes[label][w["id"], ticker] = dict(**choice, account_id=None, metrics=metrics, value=value,
                    status="abstained" if index is None else "completed" if value is not None else "unknown",
                    reason="no_eligible_is_candidate" if index is None else "nonfinite_oos" if value is None else None)
            del arrays
    for label, state in states.items():
        sources[label]["decision_access"] = [list(e) for e in state["item"].dataset.access_log[state["access_start"]:]]
    return outcomes, sources, roots, trace
