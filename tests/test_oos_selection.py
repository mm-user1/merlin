from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.testing import select_oos_source_candidates


def test_select_oos_source_prefers_stress_test():
    st_results = [
        {"trial_number": 11, "status": "ok", "st_rank": 2},
        {"trial_number": 7, "status": "ok", "st_rank": 1},
    ]
    ft_results = [{"trial_number": 3, "ft_rank": 1}]
    source, candidates = select_oos_source_candidates(
        optuna_results=[],
        dsr_results=[],
        ft_results=ft_results,
        st_results=st_results,
        st_ran=True,
    )
    assert source == "stress_test"
    assert [c["trial_number"] for c in candidates] == [11, 7]
    assert [c["source_rank"] for c in candidates] == [2, 1]


def test_select_oos_source_skips_failed_stress():
    st_results = [
        {"trial_number": 11, "status": "skipped_bad_base", "st_rank": 2},
        {"trial_number": 7, "status": "insufficient_data", "st_rank": 1},
    ]
    ft_results = [{"trial_number": 5, "ft_rank": 3}]
    source, candidates = select_oos_source_candidates(
        optuna_results=[],
        dsr_results=[],
        ft_results=ft_results,
        st_results=st_results,
        st_ran=True,
    )
    assert source == "stress_test"
    assert candidates == []


def test_select_oos_source_preserves_optuna_order():
    optuna_results = [
        {"optuna_trial_number": 7},
        {"optuna_trial_number": 3},
        {"optuna_trial_number": 9},
    ]
    source, candidates = select_oos_source_candidates(
        optuna_results=optuna_results,
        dsr_results=[],
        ft_results=[],
        st_results=[],
    )
    assert source == "optuna"
    assert [c["trial_number"] for c in candidates] == [7, 3, 9]


def test_select_oos_source_hard_vetoes_rejected_ft_candidates():
    source, candidates = select_oos_source_candidates(
        optuna_results=[{"optuna_trial_number": 1}],
        dsr_results=[{"trial_number": 2, "dsr_rank": 1}],
        ft_results=[{"trial_number": 3, "ft_rank": 1, "ft_passes_threshold": False}],
        st_results=[],
        ft_ran=True,
    )

    assert source == "forward_test"
    assert candidates == []
