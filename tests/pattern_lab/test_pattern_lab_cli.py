"""CLI contracts, import isolation and missing-dependency behavior."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

from tools.pattern_lab import data as pack_data
from tools.pattern_lab import manifest as pack_manifest
from tools.pattern_lab.__main__ import main

from ._helpers import (
    ANCHOR_MS,
    OKX_ID,
    REPO_ROOT,
    STEP_MS,
    legacy_series,
    mutate_manifest,
    sidecar,
    single_pack,
    utc,
    write_legacy_pack,
)

ISOLATION_CHILD = textwrap.dedent(
    """
    import json, sys

    observed = {"network": [], "writes": []}

    def audit(event, arguments):
        if event.startswith("socket.") or event.startswith("urllib."):
            observed["network"].append(event)
        elif event == "open":
            path, mode = arguments[0], arguments[1]
            if isinstance(mode, str) and any(flag in mode for flag in "wxa+"):
                observed["writes"].append(str(path))

    sys.addaudithook(audit)

    import tools.pattern_lab
    from tools.pattern_lab import data, import_npz, manifest
    data.require_pyarrow()
    data.input_fingerprint(
        header=data.fingerprint_header(
            instrument_id="TEST_A", venue="TEST", contract="A", quote_currency="USDT",
            timeframe_minutes=5, start_ms=0, end_ms=300000, warmup_start_ms=0,
        ),
        timestamps=[0], ohlcv=[[1.0, 1.0, 1.0, 1.0, 0.0]],
    )

    forbidden = sorted(
        name for name in sys.modules
        if name.split(".")[0] in {"flask", "werkzeug", "numba", "llvmlite", "core", "strategies", "ui", "optuna"}
    )
    print(json.dumps({"forbidden": forbidden, "observed": observed}))
    """
)

MISSING_PYARROW_CHILD = textwrap.dedent(
    """
    import io, json, sys

    attempts = []
    sys.addaudithook(
        lambda event, arguments: attempts.append(event)
        if event.startswith("subprocess.") or event.startswith("socket.") else None
    )

    class Blocker:
        def find_spec(self, name, path=None, target=None):
            if name == "pyarrow" or name.startswith("pyarrow."):
                raise ImportError("pyarrow is blocked by the test")
            return None

    sys.meta_path.insert(0, Blocker())

    from tools.pattern_lab.__main__ import main

    out, err = io.StringIO(), io.StringIO()
    stdout, stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = out, err
    try:
        code = main(["inspect", "--data-root", sys.argv[1]])
    finally:
        sys.stdout, sys.stderr = stdout, stderr
    print(json.dumps({
        "code": code,
        "stdout": out.getvalue(),
        "stderr": err.getvalue(),
        "installer_loaded": any(name.split(".")[0] in {"pip", "ensurepip"} for name in sys.modules),
        "attempts": attempts,
    }))
    """
)

UNRELATED_IMPORT_CHILD = textwrap.dedent(
    """
    import json, os, sys
    sys.path.insert(0, os.path.join(os.getcwd(), "src"))
    from core import metrics, storage
    print(json.dumps({
        "pattern_lab": [name for name in sys.modules if name.startswith("tools.pattern_lab")],
        "metrics": bool(metrics),
        "storage": bool(storage),
    }))
    """
)


def run_child(script, *arguments):
    return subprocess.run(
        [sys.executable, "-B", "-c", script, *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def run_cli(*arguments):
    return subprocess.run(
        [sys.executable, "-B", "-m", "tools.pattern_lab", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class TestCommandLine:
    def test_help_lists_the_implemented_data_commands(self):
        result = run_cli("--help")
        assert result.returncode == 0
        for command in ("import-npz", "inspect", "slice"):
            assert command in result.stdout

    def test_slice_prints_the_same_metadata_as_the_api(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=288)
        start, end = utc(ANCHOR_MS + 12 * STEP_MS), utc(ANCHOR_MS + 72 * STEP_MS)
        result = run_cli(
            "slice",
            "--data-root", str(root),
            "--instrument", "TEST_AAA-USDT-SWAP",
            "--start", start,
            "--end", end,
            "--warmup-start", utc(ANCHOR_MS),
            "--timeframe-minutes", "30",
        )
        assert result.returncode == 0, result.stderr
        assert result.stderr == ""
        payload = json.loads(result.stdout)
        expected = pack_data.slice_metadata(
            pack_data.load_slice(
                root,
                "TEST_AAA-USDT-SWAP",
                start=start,
                end=end,
                warmup_start=utc(ANCHOR_MS),
                timeframe_minutes=30,
            )
        )
        assert payload == expected
        assert payload["input_fingerprint"] == expected["input_fingerprint"]
        assert "open" not in payload  # slice never dumps rows

        # Provenance names what it is: the manifest's declared digest, not a
        # digest of the bytes this read verified.
        provenance = payload["physical_provenance"]
        assert "file_sha256" not in provenance
        assert provenance["declared_file_sha256"] == pack_manifest.read_manifest(root)["instruments"][0]["sha256"]

    def test_inspect_verify_reports_problems_with_a_nonzero_status(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=24)
        assert run_cli("inspect", "--data-root", str(root), "--verify").returncode == 0

        mutate_manifest(root, lambda manifest: manifest["instruments"][0].__setitem__("sha256", "c" * 64))
        result = run_cli("inspect", "--data-root", str(root), "--verify")
        assert result.returncode == 1
        assert "verification problem" in result.stderr
        assert json.loads(result.stdout)["verification_check"]["ok"] is False

    def test_errors_go_to_stderr_with_a_nonzero_status(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=24)
        result = run_cli(
            "slice",
            "--data-root", str(root),
            "--instrument", "TEST_MISSING",
            "--start", utc(ANCHOR_MS),
            "--end", utc(ANCHOR_MS + STEP_MS),
        )
        assert result.returncode == 2
        assert result.stdout == ""
        assert "TEST_MISSING" in result.stderr

    def test_import_emits_a_concise_summary(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()])
        metadata_path = tmp_path / "metadata.json"
        metadata_path.write_text(json.dumps(sidecar()), encoding="utf-8", newline="\n")
        output = tmp_path / "pack"
        result = run_cli(
            "import-npz",
            "--source-root", str(source),
            "--output-root", str(output),
            "--source-metadata", str(metadata_path),
            "--note", "synthetic import",
        )
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["instrument_count"] == 1
        assert payload["instruments"][0]["instrument_id"] == OKX_ID
        assert payload["source_manifest_sha256"] == pack_manifest.file_sha256(source / "MANIFEST.json")
        record = json.loads((output / "updates.jsonl").read_text(encoding="utf-8").strip())
        assert record["note"] == "synthetic import"
        assert record["source"]["kind"] == "legacy_npz_import"

    def test_import_refuses_an_existing_output_root(self, tmp_path):
        source = write_legacy_pack(tmp_path / "legacy", [legacy_series()])
        metadata_path = tmp_path / "metadata.json"
        metadata_path.write_text(json.dumps(sidecar()), encoding="utf-8", newline="\n")
        output = tmp_path / "pack"
        output.mkdir()
        assert main(
            [
                "import-npz",
                "--source-root", str(source),
                "--output-root", str(output),
                "--source-metadata", str(metadata_path),
            ]
        ) == 2


class TestImportIsolation:
    def test_clean_import_has_no_application_network_or_write_side_effects(self, tmp_path):
        result = run_child(ISOLATION_CHILD)
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["forbidden"] == []
        assert payload["observed"]["network"] == []
        inside_repository = [
            path for path in payload["observed"]["writes"] if str(REPO_ROOT) in path
        ]
        assert inside_repository == []

    def test_unrelated_merlin_imports_do_not_reach_pattern_lab(self):
        """Merlin code never imports Pattern Lab; the dependency stays one-way.

        Installing PyArrow does not make it a Merlin runtime requirement: pandas
        itself imports PyArrow when it is present, which is pandas behavior and
        independent of this package.
        """
        result = run_child(UNRELATED_IMPORT_CHILD)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["pattern_lab"] == []

    def test_pyarrow_is_referenced_only_by_pattern_lab_sources(self):
        owned = (REPO_ROOT / "tools" / "pattern_lab", REPO_ROOT / "tests" / "pattern_lab")
        offenders = []
        for path in list((REPO_ROOT / "src").rglob("*.py")) + list((REPO_ROOT / "tools").rglob("*.py")) + list(
            (REPO_ROOT / "tests").rglob("*.py")
        ):
            if any(path.is_relative_to(directory) for directory in owned):
                continue
            text = path.read_text(encoding="utf-8")
            if "pyarrow" in text or "pattern_lab" in text:
                offenders.append(str(path.relative_to(REPO_ROOT)))
        assert offenders == []

    def test_missing_pyarrow_is_actionable_nonzero_and_installs_nothing(self, tmp_path):
        root = tmp_path / "pack"
        single_pack(root, slot_count=12)
        result = run_child(MISSING_PYARROW_CHILD, str(root))
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["code"] == 2
        assert payload["stdout"] == ""
        assert "pyarrow==22.0.0" in payload["stderr"]
        assert "never installs dependencies automatically" in payload["stderr"]
        assert payload["installer_loaded"] is False
        assert payload["attempts"] == []

    def test_root_requirements_pin_the_new_dependency(self):
        requirements = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
        assert "pyarrow==22.0.0" in requirements
        assert len([line for line in requirements if line.startswith("pyarrow")]) == 1
