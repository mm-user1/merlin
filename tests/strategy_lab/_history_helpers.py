"""Ordinary extended real-engine fixtures for historical selection."""
import pytest

from _selection_helpers import publish, universe, document
from _filter_helpers import publish_filter, fixed_request, is_request
from tools.strategy_lab.selection import prepare_history_plan, write_history_fixed_request
from tools.strategy_lab.fixed.run import run
from tools.strategy_lab.generate import generate_dataset


def history_request(*, mode="fixed", end="2024-08-31", evaluation="2024-05-01", k=2, draws=32, methods=None, **geometry):
    methods = methods or ["prior_oos_positive_count", "prior_oos_mean"]
    return dict(schema_version="strategy_lab_history_request_v1", name="synthetic-history", calendar=dict(
        mode=mode, start="2024-01-01", end=end, tradingTimeframeMinutes=60,
        **(dict(periodMonths=1) if mode == "fixed" else dict(isMonths=4, oosMonths=1)), **geometry),
        evaluationStart=evaluation, historyWindows=4, topN=k, methods=methods, referenceMethod=methods[0],
        controls=dict(seed=17, draws=draws))


def plan(root, tickers, request=None, name="plan"):
    prepare_history_plan(document(root, name+"-request.json", request or history_request()), root/name,
        ticker_list=universe(root, tickers))
    return root/name


@pytest.fixture(scope="module")
def history_family(tmp_path_factory):
    work = tmp_path_factory.mktemp("history-family")
    pack = work/"pack"
    tickers = publish(pack, count=4, end="2025-01-01", quiet_before="2024-05-01")
    filters = publish_filter(work/"filters", start="2023-12-01", end="2025-01-01")
    plans = dict(fixed=plan(work,tickers), is_oos=plan(work,tickers, history_request(mode="is_oos", end="2024-12-31",
        evaluation="2024-09-01", methods=["prior_oos_positive_count", "prior_oos_mean", "selected_is_net_profit", "transfer_consistency_4w_v1"]), "candidate-plan"))
    fixed, candidates = {}, {}
    for label, filter_root in (("off",None),("on",filters)):
        raw = fixed_request(filter_root, n=2, m=1, timeframe=60)
        raw["generation"]["market_data"]["instruments"] = [dict(instrument_id=t,cell="research") for t in tickers]
        raw["generation"]["expected_run_count"] = len(tickers)
        template = document(work,label+"-template.json",raw)
        transform = work/(label+"-transform")
        write_history_fixed_request(plans["fixed"],template,transform,warmup_bars=100)
        fixed[label] = work/(label+"-fixed")
        run(transform/"fixed-request.json",pack,fixed[label])
        raw = is_request(filter_root, n=2, m=1, timeframe=60)
        if label == "off":
            raw["schema_version"] = "strategy_lab_runspec_v2"
            raw["generation"]["execution"] = dict(policy="risk_bracket_v1",entry_filters=[])
        g = raw["generation"]
        g["market_data"]["instruments"] = [dict(instrument_id=t,cell="research") for t in tickers]
        g["windows"].update(requested_start="2024-01-01",requested_end="2025-01-01",is_period_months=4,
            oos_period_months=1,expected_window_count=8)
        raw["preregistration"]["analysis_scopes"] = [dict(name=name,ticker_cell="research",window_numbers=ids,requires_unlock=locked)
            for name,ids,locked in (("research",list(range(1,9)),False),("development",list(range(1,7)),False),
                ("temporal_dev",[7,8],True),("locked_history",list(range(1,7)),True))]
        candidates[label] = work/(label+"-candidates")
        generate_dataset(document(work,label+"-is.json",raw),data_root=pack,output_dir=candidates[label])
    policy = document(work,"policy.json",dict(schema_version="strategy_lab_candidate_policy_v1",name="traded-profit",version="1",
        objective=dict(metric="net_profit_pct",direction="maximize"),constraints=[dict(metric="total_trades",operator=">=",value=1)],
        selection_count=1,no_eligible="unavailable",evidence_classification="exploratory"))
    return dict(work=work,pack=pack,filters=filters,tickers=tickers,plans=plans,fixed=fixed,candidates=candidates,policy=policy)
