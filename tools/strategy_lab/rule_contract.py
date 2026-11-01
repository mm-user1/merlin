"""Historical rules-v1 semantics, independent of current generation selection."""


def rules_v1():
    """Return a fresh definition; callers cannot mutate the supported historical contract."""
    return {
        "version": "strategy_lab_rules_v1",
        "minimum_completed_trades": 15,
        "baseline_rule": "primary_profit",
        "selectable_rules": [
            "primary_profit",
            "trade_gate15_profit",
            "trade_gate15_profit_factor",
            "trade_gate15_daily_sharpe",
            "trade_gate15_romad_mtm",
            "star_mean_profit",
            "star_worst_profit",
            "balanced_percentile_raw",
            "balanced_percentile_star",
        ],
        "nomination_eligible_rules": [
            "trade_gate15_profit",
            "trade_gate15_profit_factor",
            "trade_gate15_daily_sharpe",
            "trade_gate15_romad_mtm",
            "star_mean_profit",
            "star_worst_profit",
            "balanced_percentile_raw",
            "balanced_percentile_star",
        ],
        "non_nominatable_diagnostics": [
            "trade_gate15_romad_realized",
            "balanced_percentile_raw_realized",
            "balanced_percentile_star_realized",
            "pareto_plus_primary",
            "population_no_skill",
            "oos_oracle",
            "oos_anti_oracle",
        ],
        "tie_break": [
            {"field": "rule_score", "direction": "descending"},
            {"field": "IS net_profit_pct", "direction": "descending"},
            {"field": "semantic_key", "direction": "ascending"},
            {"field": "candidate_id", "direction": "ascending"},
        ],
    }
