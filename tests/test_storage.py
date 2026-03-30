import json
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.storage import (
    create_new_db,
    create_study_set,
    delete_study_sets,
    get_active_db_name,
    get_or_build_all_studies_analytics_cache,
    get_or_build_study_set_analytics_cache,
    get_db_connection,
    list_study_sets,
    list_study_sets_with_analytics_cache,
    load_study_from_db,
    load_wfa_window_trials,
    reorder_study_sets,
    save_wfa_study_to_db,
    set_active_db,
    update_study_sets_color,
    update_study_set,
)
from core.metrics import _calculate_r2_consistency
from core.post_process import PostProcessConfig
from core.walkforward_engine import OOSStitchedResult, WFConfig, WFResult, WindowResult


@contextmanager
def _temporary_active_db(label: str):
    previous_db = get_active_db_name()
    create_new_db(label)
    try:
        yield
    finally:
        set_active_db(previous_db)


def _build_dummy_wfa_result():
    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=10, oos_period_days=5)
    params = {"maType": "EMA", "maLength": 50, "closeCountLong": 7}

    window = WindowResult(
        window_id=1,
        is_start=pd.Timestamp("2025-01-01", tz="UTC"),
        is_end=pd.Timestamp("2025-01-10", tz="UTC"),
        oos_start=pd.Timestamp("2025-01-11", tz="UTC"),
        oos_end=pd.Timestamp("2025-01-15", tz="UTC"),
        best_params=params,
        param_id="EMA 50_test",
        is_net_profit_pct=1.0,
        is_max_drawdown_pct=0.5,
        is_total_trades=1,
        oos_net_profit_pct=2.0,
        oos_max_drawdown_pct=0.7,
        oos_total_trades=2,
        oos_winning_trades=1,
        oos_equity_curve=[100.0, 102.0],
        oos_timestamps=[
            pd.Timestamp("2025-01-11", tz="UTC"),
            pd.Timestamp("2025-01-15", tz="UTC"),
        ],
        is_best_trial_number=1,
        is_equity_curve=[100.0, 101.0],
        is_timestamps=[
            pd.Timestamp("2025-01-01", tz="UTC"),
            pd.Timestamp("2025-01-10", tz="UTC"),
        ],
        best_params_source="optuna_is",
        available_modules=["optuna_is"],
        is_pareto_optimal=True,
        constraints_satisfied=False,
        is_win_rate=50.0,
        oos_win_rate=50.0,
        optuna_is_trials=[
            {
                "trial_number": 1,
                "params": params,
                "param_id": "EMA 50_test",
                "net_profit_pct": 1.0,
                "max_drawdown_pct": 0.5,
                "total_trades": 1,
                "win_rate": 50.0,
                "is_selected": True,
            }
        ],
    )

    stitched = OOSStitchedResult(
        final_net_profit_pct=2.0,
        max_drawdown_pct=0.7,
        total_trades=2,
        wfe=100.0,
        oos_win_rate=100.0,
        equity_curve=[100.0, 102.0],
        timestamps=[
            pd.Timestamp("2025-01-11", tz="UTC"),
            pd.Timestamp("2025-01-15", tz="UTC"),
        ],
        window_ids=[1, 1],
    )

    wf_result = WFResult(
        config=wf_config,
        windows=[window],
        stitched_oos=stitched,
        strategy_id="s01_trailing_ma",
        total_windows=1,
        trading_start_date=window.is_start,
        trading_end_date=window.oos_end,
        warmup_bars=wf_config.warmup_bars,
    )
    return wf_result


def test_wfa_window_trials_table_created():
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='wfa_window_trials'"
        )
        assert cursor.fetchone() is not None


def test_study_sets_tables_created():
    with get_db_connection() as conn:
        sets_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='study_sets'"
        ).fetchone()
        members_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='study_set_members'"
        ).fetchone()
        analytics_cache_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='analytics_group_cache'"
        ).fetchone()
        set_columns = {row["name"] for row in conn.execute("PRAGMA table_info(study_sets)").fetchall()}
        analytics_cache_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(analytics_group_cache)").fetchall()
        }
        assert sets_table is not None
        assert members_table is not None
        assert analytics_cache_table is not None
        assert "color_token" in set_columns
        assert "group_key" in analytics_cache_columns
        assert "group_type" in analytics_cache_columns
        assert "set_id" in analytics_cache_columns
        assert "members_hash" in analytics_cache_columns
        assert "curve_json" in analytics_cache_columns
        assert "timestamps_json" in analytics_cache_columns
        assert "ann_profit_pct" in analytics_cache_columns
        assert "profit_pct" in analytics_cache_columns
        assert "max_drawdown_pct" in analytics_cache_columns
        assert "consistency_full" in analytics_cache_columns
        assert "consistency_recent" in analytics_cache_columns
        assert "computed_at" in analytics_cache_columns


def test_wfa_window_new_columns():
    with get_db_connection() as conn:
        cursor = conn.execute("PRAGMA table_info(wfa_windows)")
        columns = {row["name"] for row in cursor.fetchall()}
    assert "best_params_source" in columns
    assert "available_modules" in columns
    assert "optimization_start_date" in columns
    assert "optimization_start_ts" in columns
    assert "ft_start_date" in columns
    assert "ft_start_ts" in columns
    assert "is_pareto_optimal" in columns
    assert "constraints_satisfied" in columns
    assert "is_start_ts" in columns
    assert "is_end_ts" in columns
    assert "oos_start_ts" in columns
    assert "oos_end_ts" in columns
    assert "trigger_type" in columns
    assert "cusum_final" in columns
    assert "cusum_threshold" in columns
    assert "dd_threshold" in columns
    assert "oos_actual_days" in columns
    assert "cooldown_days_applied" in columns
    assert "oos_elapsed_days" in columns
    assert "oos_winning_trades" in columns
    assert "trade_start_date" in columns
    assert "trade_end_date" in columns
    assert "trade_start_ts" in columns
    assert "trade_end_ts" in columns
    assert "entry_delay_days" in columns
    assert "ft_retry_attempts_used" in columns
    assert "remaining_oos_days_at_entry" in columns
    assert "window_status" in columns
    assert "no_trade_reason" in columns


def test_studies_stitched_columns():
    with get_db_connection() as conn:
        cursor = conn.execute("PRAGMA table_info(studies)")
        columns = {row["name"] for row in cursor.fetchall()}
    assert "stitched_oos_equity_curve" in columns
    assert "stitched_oos_timestamps_json" in columns
    assert "stitched_oos_window_ids_json" in columns
    assert "stitched_oos_net_profit_pct" in columns
    assert "stitched_oos_max_drawdown_pct" in columns
    assert "stitched_oos_total_trades" in columns
    assert "stitched_oos_winning_trades" in columns
    assert "stitched_oos_win_rate" in columns
    assert "profitable_windows" in columns
    assert "total_windows" in columns
    assert "median_window_profit" in columns
    assert "median_window_wr" in columns
    assert "worst_window_profit" in columns
    assert "worst_window_dd" in columns
    assert "adaptive_mode" in columns
    assert "max_oos_period_days" in columns
    assert "min_oos_trades" in columns
    assert "check_interval_trades" in columns
    assert "cusum_threshold" in columns
    assert "dd_threshold_multiplier" in columns
    assert "inactivity_multiplier" in columns
    assert "cooldown_enabled" in columns
    assert "cooldown_days" in columns
    assert "ft_threshold_pct" in columns
    assert "ft_reject_action" in columns
    assert "ft_reject_cooldown_days" in columns
    assert "ft_reject_max_attempts" in columns
    assert "ft_reject_min_remaining_oos_days" in columns
    assert "stitched_oos_start_ts" in columns
    assert "stitched_oos_end_ts" in columns
    assert "stitched_oos_point_count" in columns


def test_trials_ft_gate_columns_exist():
    with get_db_connection() as conn:
        cursor = conn.execute("PRAGMA table_info(trials)")
        columns = {row["name"] for row in cursor.fetchall()}

    assert "ft_passes_threshold" in columns


def test_save_wfa_study_with_trials():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    study_data = load_study_from_db(study_id)
    assert study_data is not None
    assert study_data["study"]["optimization_mode"] == "wfa"
    assert study_data["windows"]

    window = study_data["windows"][0]
    assert window.get("best_params_source") == "optuna_is"
    assert window.get("is_pareto_optimal") is True
    assert window.get("constraints_satisfied") is False
    assert window.get("oos_winning_trades") == 1

    study = study_data["study"]
    assert study.get("stitched_oos_winning_trades") == 1
    assert study.get("profitable_windows") == 1
    assert study.get("total_windows") == 1
    assert study.get("median_window_profit") == 2.0
    assert study.get("median_window_wr") == 50.0
    assert study.get("worst_window_profit") == 2.0
    assert study.get("worst_window_dd") == 0.7
    assert study.get("stitched_oos_start_ts") == "2025-01-11T00:00:00+00:00"
    assert study.get("stitched_oos_end_ts") == "2025-01-15T00:00:00+00:00"
    assert study.get("stitched_oos_point_count") == 2


def test_study_sets_storage_roundtrip():
    wf_result_a = _build_dummy_wfa_result()
    study_id_a = save_wfa_study_to_db(
        wf_result=wf_result_a,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    wf_result_b = _build_dummy_wfa_result()
    wf_result_b.windows[0].window_id = 2
    study_id_b = save_wfa_study_to_db(
        wf_result=wf_result_b,
        config={},
        csv_file_path="",
        start_time=time.time(),
        score_config=None,
    )

    created = create_study_set("Storage Roundtrip Set", [study_id_a, study_id_b])
    assert created["name"] == "Storage Roundtrip Set"
    assert created["color_token"] is None
    assert created["study_ids"] == [study_id_a, study_id_b]

    updated = update_study_set(
        created["id"],
        name="Storage Roundtrip Set v2",
        study_ids=[study_id_b],
        color_token="blue",
    )
    assert updated["name"] == "Storage Roundtrip Set v2"
    assert updated["color_token"] == "blue"
    assert updated["study_ids"] == [study_id_b]

    second = create_study_set("Storage Roundtrip Set v3", [study_id_a])
    reorder_study_sets([second["id"], created["id"]])

    sets = list_study_sets()
    assert [entry["id"] for entry in sets[:2]] == [second["id"], created["id"]]
    assert sets[0]["color_token"] is None
    assert sets[1]["color_token"] == "blue"


def test_study_sets_reject_invalid_color_token():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    created = create_study_set("Invalid Color Set", [study_id])
    with pytest.raises(ValueError):
        update_study_set(created["id"], color_token="magenta")


def test_study_sets_color_token_can_be_cleared():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    created = create_study_set("Clear Color Set", [study_id], color_token="teal")
    assert created["color_token"] == "teal"

    cleared = update_study_set(created["id"], color_token=None)
    assert cleared["color_token"] is None


def test_study_sets_rename_auto_suffixes_duplicate_names():
    study_id = save_wfa_study_to_db(
        wf_result=_build_dummy_wfa_result(),
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    first = create_study_set("Rename Duplicate", [study_id])
    second = create_study_set("Rename Duplicate", [study_id])
    target = create_study_set("Rename Target", [study_id])

    updated = update_study_set(target["id"], name="Rename Duplicate")
    assert updated["name"] == "Rename Duplicate (2)"

    same_name = update_study_set(second["id"], name="Rename Duplicate")
    assert same_name["name"] == "Rename Duplicate (1)"

    by_id = {entry["id"]: entry for entry in list_study_sets()}
    assert by_id[first["id"]]["name"] == "Rename Duplicate"
    assert by_id[second["id"]]["name"] == "Rename Duplicate (1)"
    assert by_id[target["id"]]["name"] == "Rename Duplicate (2)"


def test_study_sets_bulk_color_update_and_delete():
    first_study = save_wfa_study_to_db(
        wf_result=_build_dummy_wfa_result(),
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    second_study = save_wfa_study_to_db(
        wf_result=_build_dummy_wfa_result(),
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    first = create_study_set("Bulk Color First", [first_study], color_token="blue")
    second = create_study_set("Bulk Color Second", [second_study], color_token="teal")

    updated = update_study_sets_color([first["id"], second["id"]], "rose")
    assert [item["id"] for item in updated] == [first["id"], second["id"]]
    assert [item["color_token"] for item in updated] == ["rose", "rose"]

    deleted_count = delete_study_sets([first["id"], second["id"]])
    assert deleted_count == 2
    remaining_ids = {entry["id"] for entry in list_study_sets()}
    assert first["id"] not in remaining_ids
    assert second["id"] not in remaining_ids


def test_study_set_analytics_cache_roundtrip_and_invalidation():
    with _temporary_active_db("storage_cache_roundtrip"):
        first_result = _build_dummy_wfa_result()
        first_study_id = save_wfa_study_to_db(
            wf_result=first_result,
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )

        second_result = _build_dummy_wfa_result()
        second_result.windows[0].window_id = 2
        second_result.windows[0].param_id = "EMA 75_test"
        second_result.windows[0].best_params = {"maType": "EMA", "maLength": 75, "closeCountLong": 7}
        second_result.windows[0].oos_net_profit_pct = 5.0
        second_result.windows[0].oos_equity_curve = [100.0, 105.0]
        second_result.stitched_oos.final_net_profit_pct = 5.0
        second_result.stitched_oos.equity_curve = [100.0, 105.0]
        second_study_id = save_wfa_study_to_db(
            wf_result=second_result,
            config={},
            csv_file_path="",
            start_time=1.0,
            score_config=None,
        )

        created = create_study_set("Cache Set", [first_study_id, second_study_id])

        initial_cache = get_or_build_study_set_analytics_cache(created["id"])
        repeated_cache = get_or_build_study_set_analytics_cache(created["id"])
        assert initial_cache["selected_count"] == 2
        assert initial_cache["has_curve"] is True
        assert initial_cache["computed_at"] == repeated_cache["computed_at"]
        assert len(initial_cache["curve"]) == len(initial_cache["timestamps"]) == 2

        updated = update_study_set(created["id"], study_ids=[first_study_id])
        refreshed_cache = get_or_build_study_set_analytics_cache(updated["id"])
        assert refreshed_cache["selected_count"] == 1
        assert refreshed_cache["profit_pct"] == pytest.approx(2.0)
        assert refreshed_cache["computed_at"] != initial_cache["computed_at"]

        sets_payload = list_study_sets_with_analytics_cache()
        assert sets_payload["all_metrics"]["selected_count"] == 2
        assert sets_payload["sets"][0]["metrics"]["selected_count"] == 1
        assert sets_payload["sets"][0]["metrics"]["profit_pct"] == pytest.approx(2.0)


def test_study_set_analytics_cache_includes_recent_and_full_consistency():
    with _temporary_active_db("storage_cache_consistency"):
        curve = [100.0, 104.0, 108.0, 112.0, 116.0, 120.0, 119.0, 117.0, 115.0]
        timestamps = [pd.Timestamp(f"2025-01-{day:02d}", tz="UTC") for day in range(1, 10)]

        wf_result = _build_dummy_wfa_result()
        wf_result.windows[0].window_id = 11
        wf_result.windows[0].oos_equity_curve = curve
        wf_result.windows[0].oos_timestamps = timestamps
        wf_result.stitched_oos.equity_curve = curve
        wf_result.stitched_oos.timestamps = timestamps
        wf_result.stitched_oos.final_net_profit_pct = 15.0
        wf_result.stitched_oos.max_drawdown_pct = 4.1667

        study_id = save_wfa_study_to_db(
            wf_result=wf_result,
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )

        created = create_study_set("Consistency Set", [study_id])
        cache_payload = get_or_build_study_set_analytics_cache(created["id"])
        summary_payload = list_study_sets_with_analytics_cache()["sets"][0]["metrics"]

        expected_full = _calculate_r2_consistency(curve)
        expected_recent = _calculate_r2_consistency(curve[-3:])

        assert cache_payload["consistency_full"] == pytest.approx(expected_full, abs=1e-6)
        assert cache_payload["consistency_recent"] == pytest.approx(expected_recent, abs=1e-6)
        assert summary_payload["consistency_full"] == pytest.approx(expected_full, abs=1e-6)
        assert summary_payload["consistency_recent"] == pytest.approx(expected_recent, abs=1e-6)


def test_study_set_analytics_cache_legacy_rows_compute_missing_consistency():
    with _temporary_active_db("storage_cache_legacy_consistency"):
        curve = [100.0, 104.0, 108.0, 112.0, 116.0, 120.0, 119.0, 117.0, 115.0]
        timestamps = [pd.Timestamp(f"2025-02-{day:02d}", tz="UTC") for day in range(1, 10)]

        wf_result = _build_dummy_wfa_result()
        wf_result.windows[0].window_id = 12
        wf_result.windows[0].oos_equity_curve = curve
        wf_result.windows[0].oos_timestamps = timestamps
        wf_result.stitched_oos.equity_curve = curve
        wf_result.stitched_oos.timestamps = timestamps
        wf_result.stitched_oos.final_net_profit_pct = 15.0
        wf_result.stitched_oos.max_drawdown_pct = 4.1667

        study_id = save_wfa_study_to_db(
            wf_result=wf_result,
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )

        created = create_study_set("Legacy Consistency Set", [study_id])
        initial_cache = get_or_build_study_set_analytics_cache(created["id"])

        with get_db_connection() as conn:
            conn.execute(
                """
                UPDATE analytics_group_cache
                SET
                    consistency_full = NULL,
                    consistency_recent = NULL
                WHERE group_key = ?
                """,
                (f"set:{created['id']}",),
            )
            conn.commit()

        refreshed_cache = get_or_build_study_set_analytics_cache(created["id"])
        summary_payload = list_study_sets_with_analytics_cache()["sets"][0]["metrics"]

        assert refreshed_cache["computed_at"] == initial_cache["computed_at"]
        assert refreshed_cache["consistency_full"] == pytest.approx(
            initial_cache["consistency_full"],
            abs=1e-6,
        )
        assert refreshed_cache["consistency_recent"] == pytest.approx(
            initial_cache["consistency_recent"],
            abs=1e-6,
        )
        assert summary_payload["consistency_full"] == pytest.approx(
            initial_cache["consistency_full"],
            abs=1e-6,
        )
        assert summary_payload["consistency_recent"] == pytest.approx(
            initial_cache["consistency_recent"],
            abs=1e-6,
        )


def test_all_studies_analytics_cache_invalidates_after_new_wfa_study_saved():
    with _temporary_active_db("storage_all_cache_invalidation"):
        save_wfa_study_to_db(
            wf_result=_build_dummy_wfa_result(),
            config={},
            csv_file_path="",
            start_time=0.0,
            score_config=None,
        )
        first_cache = get_or_build_all_studies_analytics_cache()
        assert first_cache["selected_count"] == 1
        assert first_cache["has_curve"] is True

        second_result = _build_dummy_wfa_result()
        second_result.windows[0].window_id = 2
        second_result.windows[0].param_id = "EMA 90_test"
        second_result.windows[0].best_params = {"maType": "EMA", "maLength": 90, "closeCountLong": 7}
        save_wfa_study_to_db(
            wf_result=second_result,
            config={},
            csv_file_path="",
            start_time=2.0,
            score_config=None,
        )

        second_cache = get_or_build_all_studies_analytics_cache()
        assert second_cache["selected_count"] == 2
        assert second_cache["computed_at"] != first_cache["computed_at"]


def test_save_wfa_study_layer1_aggregates_multi_window():
    wf_config = WFConfig(strategy_id="s01_trailing_ma", is_period_days=10, oos_period_days=5)
    windows = [
        WindowResult(
            window_id=1,
            is_start=pd.Timestamp("2025-01-01", tz="UTC"),
            is_end=pd.Timestamp("2025-01-10", tz="UTC"),
            oos_start=pd.Timestamp("2025-01-11", tz="UTC"),
            oos_end=pd.Timestamp("2025-01-15", tz="UTC"),
            best_params={"maType": "EMA", "maLength": 50, "closeCountLong": 7},
            param_id="p1",
            is_net_profit_pct=1.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=2,
            oos_net_profit_pct=6.0,
            oos_max_drawdown_pct=12.0,
            oos_total_trades=4,
            oos_winning_trades=3,
            oos_equity_curve=[100.0, 106.0],
            oos_timestamps=[
                pd.Timestamp("2025-01-11", tz="UTC"),
                pd.Timestamp("2025-01-15", tz="UTC"),
            ],
            oos_win_rate=75.0,
        ),
        WindowResult(
            window_id=2,
            is_start=pd.Timestamp("2025-01-06", tz="UTC"),
            is_end=pd.Timestamp("2025-01-15", tz="UTC"),
            oos_start=pd.Timestamp("2025-01-16", tz="UTC"),
            oos_end=pd.Timestamp("2025-01-20", tz="UTC"),
            best_params={"maType": "EMA", "maLength": 50, "closeCountLong": 7},
            param_id="p1",
            is_net_profit_pct=1.0,
            is_max_drawdown_pct=1.0,
            is_total_trades=2,
            oos_net_profit_pct=-2.0,
            oos_max_drawdown_pct=30.0,
            oos_total_trades=5,
            oos_winning_trades=1,
            oos_equity_curve=[100.0, 98.0],
            oos_timestamps=[
                pd.Timestamp("2025-01-16", tz="UTC"),
                pd.Timestamp("2025-01-20", tz="UTC"),
            ],
            oos_win_rate=20.0,
        ),
    ]
    stitched = OOSStitchedResult(
        final_net_profit_pct=3.88,
        max_drawdown_pct=8.0,
        total_trades=9,
        wfe=10.0,
        oos_win_rate=50.0,
        equity_curve=[100.0, 106.0, 103.88],
        timestamps=[
            pd.Timestamp("2025-01-11", tz="UTC"),
            pd.Timestamp("2025-01-15", tz="UTC"),
            pd.Timestamp("2025-01-20", tz="UTC"),
        ],
        window_ids=[1, 1, 2],
    )
    wf_result = WFResult(
        config=wf_config,
        windows=windows,
        stitched_oos=stitched,
        strategy_id="s01_trailing_ma",
        total_windows=2,
        trading_start_date=pd.Timestamp("2025-01-01", tz="UTC"),
        trading_end_date=pd.Timestamp("2025-01-20", tz="UTC"),
        warmup_bars=wf_config.warmup_bars,
    )

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    loaded = load_study_from_db(study_id)
    assert loaded is not None
    study = loaded["study"]

    assert study.get("stitched_oos_winning_trades") == 4
    assert study.get("profitable_windows") == 1
    assert study.get("total_windows") == 2
    assert study.get("median_window_profit") == 2.0
    assert study.get("median_window_wr") == 47.5
    assert study.get("worst_window_profit") == -2.0
    assert study.get("worst_window_dd") == 30.0


def test_save_wfa_study_persists_optuna_and_wfa_metadata():
    wf_result = _build_dummy_wfa_result()
    wf_result.config.is_period_days = 12
    wf_result.config.adaptive_mode = True
    wf_result.config.max_oos_period_days = 120
    wf_result.config.min_oos_trades = 7
    wf_result.config.check_interval_trades = 4
    wf_result.config.cusum_threshold = 6.5
    wf_result.config.dd_threshold_multiplier = 1.8
    wf_result.config.inactivity_multiplier = 6.0
    wf_result.config.cooldown_enabled = True
    wf_result.config.cooldown_days = 15
    wf_result.config.post_process = PostProcessConfig(
        enabled=True,
        ft_period_days=14,
        top_k=10,
        sort_metric="profit_degradation",
        ft_threshold_pct=-5.0,
        ft_reject_action="cooldown_reoptimize",
        ft_reject_cooldown_days=5,
        ft_reject_max_attempts=2,
        ft_reject_min_remaining_oos_days=10,
    )
    wf_result.windows[0].trigger_type = "cusum"
    wf_result.windows[0].oos_actual_days = 4.0
    wf_result.windows[0].cooldown_days_applied = 15.0
    wf_result.windows[0].oos_elapsed_days = 19.0
    wf_result.windows[0].trade_start = pd.Timestamp("2025-01-13", tz="UTC")
    wf_result.windows[0].trade_end = pd.Timestamp("2025-01-15", tz="UTC")
    wf_result.windows[0].entry_delay_days = 2.0
    wf_result.windows[0].ft_retry_attempts_used = 1
    wf_result.windows[0].remaining_oos_days_at_entry = 3.0
    wf_result.windows[0].window_status = "traded"

    config = {
        "sampler_type": "nsga2",
        "population_size": 64,
        "crossover_prob": 0.8,
        "mutation_prob": 0.2,
        "swapping_prob": 0.4,
        "optuna_config": {
            "budget_mode": "trials",
            "n_trials": 300,
            "time_limit": 1800,
            "convergence_patience": 75,
            "sampler": "nsga2",
            "sampler_type": "nsga2",
            "population_size": 64,
            "crossover_prob": 0.8,
            "mutation_prob": 0.2,
            "swapping_prob": 0.4,
            "pruner": "median",
        },
        "wfa": {
            "is_period_days": 10,
            "oos_period_days": 5,
            "adaptive_mode": True,
            "cooldown_enabled": True,
            "cooldown_days": 15,
        },
    }

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config=config,
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )

    loaded = load_study_from_db(study_id)
    assert loaded is not None
    study = loaded["study"]

    assert study.get("is_period_days") == 12
    assert study.get("sampler_type") == "nsga2"
    assert study.get("population_size") == 64
    assert study.get("crossover_prob") == 0.8
    assert study.get("mutation_prob") == 0.2
    assert study.get("swapping_prob") == 0.4
    assert study.get("budget_mode") == "trials"
    assert study.get("n_trials") == 300
    assert study.get("time_limit") == 1800
    assert study.get("convergence_patience") == 75

    assert study.get("adaptive_mode") == 1
    assert study.get("max_oos_period_days") == 120
    assert study.get("min_oos_trades") == 7
    assert study.get("check_interval_trades") == 4
    assert study.get("cusum_threshold") == 6.5
    assert study.get("dd_threshold_multiplier") == 1.8
    assert study.get("inactivity_multiplier") == 6.0
    assert study.get("cooldown_enabled") == 1
    assert study.get("cooldown_days") == 15
    assert study.get("ft_enabled") == 1
    assert study.get("ft_period_days") == 14
    assert study.get("ft_top_k") == 10
    assert study.get("ft_sort_metric") == "profit_degradation"
    assert study.get("ft_threshold_pct") == -5.0
    assert study.get("ft_reject_action") == "cooldown_reoptimize"
    assert study.get("ft_reject_cooldown_days") == 5
    assert study.get("ft_reject_max_attempts") == 2
    assert study.get("ft_reject_min_remaining_oos_days") == 10

    config_json = study.get("config_json") or {}
    assert config_json.get("optuna_config", {}).get("pruner") == "median"
    assert config_json.get("wfa", {}).get("oos_period_days") == 5
    assert config_json.get("wfa", {}).get("cooldown_enabled") is True
    assert config_json.get("wfa", {}).get("cooldown_days") == 15

    window = loaded["windows"][0]
    assert window.get("trigger_type") == "cusum"
    assert window.get("oos_actual_days") == 4.0
    assert window.get("cooldown_days_applied") == 15.0
    assert window.get("oos_elapsed_days") == 19.0
    assert window.get("trade_start_ts") == "2025-01-13T00:00:00+00:00"
    assert window.get("trade_end_ts") == "2025-01-15T00:00:00+00:00"
    assert window.get("entry_delay_days") == 2.0
    assert window.get("ft_retry_attempts_used") == 1
    assert window.get("remaining_oos_days_at_entry") == 3.0
    assert window.get("window_status") == "traded"


def test_save_wfa_study_persists_runtime_seconds():
    wf_result = _build_dummy_wfa_result()
    start_time = time.time() - 2.0
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=start_time,
        score_config=None,
    )

    loaded = load_study_from_db(study_id)
    assert loaded is not None
    runtime = loaded["study"].get("optimization_time_seconds")
    assert runtime is not None
    assert runtime >= 0
    assert runtime < 600


def test_load_wfa_window_trials():
    wf_result = _build_dummy_wfa_result()
    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    window_id = f"{study_id}_w1"
    modules = load_wfa_window_trials(window_id)
    assert "optuna_is" in modules
    assert modules["optuna_is"]
    assert modules["optuna_is"][0]["trial_number"] == 1


def test_wfa_window_timestamp_precision_persisted():
    wf_result = _build_dummy_wfa_result()
    window = wf_result.windows[0]
    window.is_start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    window.is_end = pd.Timestamp("2025-01-10 09:15:00", tz="UTC")
    window.oos_start = pd.Timestamp("2025-01-11 06:45:00", tz="UTC")
    window.oos_end = pd.Timestamp("2025-01-15 12:30:00", tz="UTC")
    window.optimization_start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    window.optimization_end = pd.Timestamp("2025-01-09 23:00:00", tz="UTC")
    window.ft_start = pd.Timestamp("2025-01-09 23:00:00", tz="UTC")
    window.ft_end = pd.Timestamp("2025-01-10 09:15:00", tz="UTC")

    study_id = save_wfa_study_to_db(
        wf_result=wf_result,
        config={},
        csv_file_path="",
        start_time=0.0,
        score_config=None,
    )
    loaded = load_study_from_db(study_id)
    assert loaded is not None
    stored = loaded["windows"][0]

    assert stored.get("is_start_ts") == "2025-01-01T00:00:00+00:00"
    assert stored.get("is_end_ts") == "2025-01-10T09:15:00+00:00"
    assert stored.get("oos_start_ts") == "2025-01-11T06:45:00+00:00"
    assert stored.get("oos_end_ts") == "2025-01-15T12:30:00+00:00"
    assert stored.get("optimization_start_ts") == "2025-01-01T00:00:00+00:00"
    assert stored.get("optimization_end_ts") == "2025-01-09T23:00:00+00:00"
    assert stored.get("ft_start_ts") == "2025-01-09T23:00:00+00:00"
    assert stored.get("ft_end_ts") == "2025-01-10T09:15:00+00:00"
