from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def restore_numba_disable_jit_state():
    """Keep V1 oracle tests from leaking process-global JIT settings."""

    original_env = os.environ.get("NUMBA_DISABLE_JIT")
    numba_module = None
    original_config_disable_jit = None
    try:
        import numba

        numba_module = numba
        original_config_disable_jit = bool(numba.config.DISABLE_JIT)
    except Exception:
        numba_module = None

    try:
        yield
    finally:
        if original_env is None:
            os.environ.pop("NUMBA_DISABLE_JIT", None)
        else:
            os.environ["NUMBA_DISABLE_JIT"] = original_env
        if numba_module is not None and original_config_disable_jit is not None:
            numba_module.config.DISABLE_JIT = original_config_disable_jit
