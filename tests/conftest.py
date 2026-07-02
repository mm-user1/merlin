import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core import storage


@pytest.fixture(scope="session", autouse=True)
def isolate_storage_for_test_session(tmp_path_factory):
    """
    Route all test DB/JOURNAL writes into a temporary directory.

    This prevents test runs from polluting real DB files under src/storage.
    The temporary root lives under pytest's session tmp directory (outside the
    repository), so a failed or interrupted run never leaves artifacts under
    ``tests/``.  Pytest owns retention/cleanup of that directory.
    """
    root = tmp_path_factory.mktemp("merlin_test_storage").resolve()
    storage_dir = root / "storage"
    journal_dir = storage_dir / "journals"
    storage_dir.mkdir(parents=True, exist_ok=True)
    journal_dir.mkdir(parents=True, exist_ok=True)

    original_storage_dir = storage.STORAGE_DIR
    original_journal_dir = storage.JOURNAL_DIR
    original_active_db_path = storage._active_db_path
    original_db_initialized = storage.DB_INITIALIZED

    optuna_engine = None
    original_optuna_journal_dir = None
    try:
        from core import optuna_engine as _optuna_engine

        optuna_engine = _optuna_engine
        original_optuna_journal_dir = _optuna_engine.JOURNAL_DIR
    except Exception:
        optuna_engine = None
        original_optuna_journal_dir = None

    storage.STORAGE_DIR = storage_dir
    storage.JOURNAL_DIR = journal_dir
    storage._active_db_path = storage_dir / "tests_session.db"
    storage.DB_INITIALIZED = False

    if optuna_engine is not None:
        optuna_engine.JOURNAL_DIR = journal_dir

    # Seed the default session database so tests that temporarily switch active DBs
    # can always restore back to an existing file regardless of test order.
    storage.init_database(db_path=storage._active_db_path)

    try:
        yield
    finally:
        storage.DB_INITIALIZED = False
        storage.STORAGE_DIR = original_storage_dir
        storage.JOURNAL_DIR = original_journal_dir
        storage._active_db_path = original_active_db_path
        storage.DB_INITIALIZED = original_db_initialized

        if optuna_engine is not None and original_optuna_journal_dir is not None:
            optuna_engine.JOURNAL_DIR = original_optuna_journal_dir


@pytest.fixture(scope="session", autouse=True)
def allow_test_csv_roots(tmp_path_factory):
    """
    In tests, permit CSV paths under the repository working directory and under
    pytest's temporary root.

    Production policy still uses configured roots; this only adjusts test runtime
    globals to keep fixtures, sample data, and per-test ``tmp_path`` CSV files
    accessible without leaving artifacts inside the repository.
    """
    try:
        from ui import server_services
    except Exception:
        yield
        return

    original_roots = list(server_services.CSV_ALLOWED_ROOTS)
    try:
        server_services.CSV_ALLOWED_ROOTS = [
            Path.cwd().resolve(),
            tmp_path_factory.getbasetemp().resolve(),
        ]
        yield
    finally:
        server_services.CSV_ALLOWED_ROOTS = original_roots
