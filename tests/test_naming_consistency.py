"""Test naming consistency across the codebase.

Ensures all parameters use camelCase throughout the system,
preventing regression to snake_case naming.
"""
import inspect
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from strategies import get_strategy_config
from strategies.s01_trailing_ma.strategy import S01Params
from strategies.s04_stochrsi.strategy import S04Params
from strategies.s06_r_trend_v02.strategy import S06Params


class TestParameterNaming:
    """Test parameter naming conventions."""

    def test_s01_params_use_camelCase(self):
        """Verify all S01Params fields use camelCase (no underscores)."""
        internal_params = {"use_backtester", "use_date_filter", "start", "end"}

        for field in fields(S01Params):
            if field.name in internal_params:
                continue

            assert "_" not in field.name, (
                f"S01Params field '{field.name}' uses snake_case. "
                f"All strategy parameters must use camelCase."
            )

    def test_s04_params_use_camelCase(self):
        """Verify all S04Params fields use camelCase (no underscores)."""
        internal_params = {
            "use_backtester",
            "use_date_filter",
            "start",
            "end",
            "startDate",
            "endDate",
        }

        for field in fields(S04Params):
            if field.name in internal_params:
                continue

            assert "_" not in field.name, (
                f"S04Params field '{field.name}' uses snake_case. "
                f"All strategy parameters must use camelCase."
            )

    def test_s06_params_use_camelCase(self):
        internal_params = {"use_date_filter", "start", "end"}

        for field in fields(S06Params):
            if field.name in internal_params:
                continue
            assert "_" not in field.name, (
                f"S06Params field '{field.name}' uses snake_case. "
                f"All strategy parameters must use camelCase."
            )


class TestConfigParameterConsistency:
    """Test config.json matches Python Params dataclasses."""

    def test_s01_config_matches_params(self):
        """Verify S01 config.json parameter names match S01Params dataclass."""
        config = get_strategy_config("s01_trailing_ma")
        config_params = set(config["parameters"].keys())

        internal_params = {"use_backtester", "use_date_filter", "start", "end"}
        dataclass_params = {f.name for f in fields(S01Params) if f.name not in internal_params}

        for param_name in config_params:
            assert param_name in dataclass_params, (
                f"Config param '{param_name}' not found in S01Params dataclass"
            )

        for param_name in dataclass_params:
            assert param_name in config_params, (
                f"Dataclass param '{param_name}' not found in S01 config.json"
            )

    def test_s04_config_matches_params(self):
        """Verify S04 config.json parameter names match S04Params dataclass."""
        config = get_strategy_config("s04_stochrsi")
        config_params = set(config["parameters"].keys())

        internal_params = {
            "use_backtester",
            "use_date_filter",
            "start",
            "end",
            "startDate",
            "endDate",
        }
        dataclass_params = {f.name for f in fields(S04Params) if f.name not in internal_params}

        for param_name in config_params:
            assert param_name in dataclass_params, (
                f"Config param '{param_name}' not found in S04Params dataclass"
            )

        for param_name in dataclass_params:
            assert param_name in config_params, (
                f"Dataclass param '{param_name}' not found in S04 config.json"
            )

    def test_s06_config_matches_params(self):
        config = get_strategy_config("s06_r_trend_v02")
        config_params = set(config["parameters"].keys())
        internal_params = {"use_date_filter", "start", "end"}
        dataclass_params = {f.name for f in fields(S06Params) if f.name not in internal_params}

        assert config_params == dataclass_params


class TestNoConversionCode:
    """Test that no snake_case ↔ camelCase conversion exists."""

    def test_no_to_dict_method(self):
        """Verify Params dataclasses don't have to_dict() method."""
        assert not hasattr(S01Params, "to_dict"), (
            "S01Params should not have to_dict() method. Use asdict() instead."
        )
        assert not hasattr(S04Params, "to_dict"), (
            "S04Params should not have to_dict() method. Use asdict() instead."
        )
        assert not hasattr(S06Params, "to_dict"), (
            "S06Params should not have to_dict() method. Use asdict() instead."
        )

    def test_from_dict_no_conversion(self):
        """Verify from_dict() uses direct mapping (no conversion)."""
        s01_source = inspect.getsource(S01Params.from_dict)
        assert "ma_type" not in s01_source
        assert "ma_length" not in s01_source
        assert "close_count_long" not in s01_source

        s04_source = inspect.getsource(S04Params.from_dict)
        assert "rsi_len" not in s04_source


class TestParameterTypes:
    """Test parameter type definitions are valid."""

    VALID_TYPES = {"int", "float", "select", "options", "bool", "boolean"}

    def test_s01_parameter_types_valid(self):
        """Verify all S01 parameters use valid types."""
        config = get_strategy_config("s01_trailing_ma")
        parameters: Dict[str, Dict[str, Any]] = config.get("parameters", {})

        for param_name, param_spec in parameters.items():
            param_type = param_spec.get("type")
            assert param_type in self.VALID_TYPES, (
                f"S01 param '{param_name}' has invalid type '{param_type}'. "
                f"Valid types: {self.VALID_TYPES}"
            )

    def test_s04_parameter_types_valid(self):
        """Verify all S04 parameters use valid types."""
        config = get_strategy_config("s04_stochrsi")
        parameters: Dict[str, Dict[str, Any]] = config.get("parameters", {})

        for param_name, param_spec in parameters.items():
            param_type = param_spec.get("type")
            assert param_type in self.VALID_TYPES, (
                f"S04 param '{param_name}' has invalid type '{param_type}'. "
                f"Valid types: {self.VALID_TYPES}"
            )

    def test_select_types_have_options(self):
        """Verify 'select' type parameters have 'options' field."""
        for strategy_id in ["s01_trailing_ma", "s04_stochrsi", "s06_r_trend_v02"]:
            config = get_strategy_config(strategy_id)
            parameters: Dict[str, Dict[str, Any]] = config.get("parameters", {})

            for param_name, param_spec in parameters.items():
                param_type = param_spec.get("type")
                if param_type in ["select", "options"]:
                    options = param_spec.get("options")
                    assert options is not None, (
                        f"{strategy_id} param '{param_name}' is type 'select' "
                        f"but has no 'options' field"
                    )
                    assert isinstance(options, list), (
                        f"{strategy_id} param '{param_name}' options must be a list"
                    )
                    assert len(options) > 0, (
                        f"{strategy_id} param '{param_name}' options list is empty"
                    )


class TestNoFeatureFlags:
    """Test that feature flags have been removed."""

    def test_s01_no_features_section(self):
        """Verify S01 config.json has no 'features' section."""
        config = get_strategy_config("s01_trailing_ma")
        assert "features" not in config, (
            "S01 config.json should not have 'features' section. "
            "Use parameter types instead (e.g., 'type': 'select')."
        )

    def test_s04_no_features_section(self):
        """Verify S04 config.json has no 'features' section."""
        config = get_strategy_config("s04_stochrsi")
        assert "features" not in config, (
            "S04 config.json should not have 'features' section."
        )

    def test_s06_no_features_section(self):
        config = get_strategy_config("s06_r_trend_v02")
        assert "features" not in config


class TestOptimizationResultStructure:
    """Test OptimizationResult uses generic structure."""

    def test_optimization_result_has_params_dict(self):
        """Verify OptimizationResult has 'params' field."""
        from core.optuna_engine import OptimizationResult
        from dataclasses import fields as get_fields

        field_names = {f.name for f in get_fields(OptimizationResult)}

        assert "params" in field_names, (
            "OptimizationResult should have 'params' field (generic dict)"
        )

    def test_optimization_result_no_s01_fields(self):
        """Verify OptimizationResult has no S01-specific parameter fields."""
        from core.optuna_engine import OptimizationResult
        from dataclasses import fields as get_fields

        field_names = {f.name for f in get_fields(OptimizationResult)}

        forbidden_fields = {
            "ma_type",
            "ma_length",
            "close_count_long",
            "close_count_short",
            "stop_long_atr",
            "trail_rr_long",
            "trail_ma_long_type",
        }

        for forbidden in forbidden_fields:
            assert forbidden not in field_names, (
                f"OptimizationResult unexpectedly contains S01-specific field '{forbidden}'"
            )
