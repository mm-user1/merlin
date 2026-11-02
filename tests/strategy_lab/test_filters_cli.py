"""Documented copied-request workflow across both families, entirely synthetic."""
import csv
from html import unescape
import json
import re
from urllib.parse import unquote

from tools.strategy_lab.config import REPO_ROOT
from tools.strategy_lab.fixed.storage import open_dataset
from _fixed_helpers import snapshot
from test_fixed_cli import cli


def test_documented_filter_workflows(tmp_path):
    pack = tmp_path / "pack"
    calls = []
    def invoke(*args):
        result = cli(*args)
        calls.append({"args": list(map(str, args)), "stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode})
        return result
    invoke("tools.strategy_lab.synthetic_parquet", "--output-dir", pack, "--symbols", "AAA", "BBB",
        "--start", "2024-01-01", "--end", "2024-05-02")
    requests = {}
    for family in ("fixed", "is_oos"):
        for state in ("off", "on"):
            name = f"s07_filters_{family}_{state}"
            raw = json.loads((REPO_ROOT / "tools/strategy_lab/runspecs" / (name+".json")).read_text())
            for item in raw["generation"]["execution"]["entryFilters"]["items"]:
                item["source"]["pack_root"] = str(pack.resolve())
            path = tmp_path / (name+".json"); path.write_text(json.dumps(raw), encoding="utf-8")
            requests[family, state] = path
    for state in ("off", "on"):
        out = tmp_path / ("fixed-"+state); path = requests["fixed", state]
        invoke("tools.strategy_lab.fixed", "validate", path)
        invoke("tools.strategy_lab.fixed", "run", path, "--data-root", pack, "--output-dir", out, "--preflight-only")
        invoke("tools.strategy_lab.fixed", "run", path, "--data-root", pack, "--output-dir", out, "--resume")
        before = snapshot(out)
        assert '"no_op": true' in invoke("tools.strategy_lab.fixed", "run", path, "--data-root", pack, "--output-dir", out).stdout
        assert snapshot(out) == before
    for state in ("off", "on"):
        out = tmp_path / ("is-"+state); path = requests["is_oos", state]
        invoke("tools.strategy_lab.config", path)
        invoke("tools.strategy_lab.generate", path, "--data-root", pack, "--output-dir", out, "--preflight-only")
        invoke("tools.strategy_lab.generate", path, "--data-root", pack, "--output-dir", out, "--resume")
    relocated = tmp_path / "relocated-pack"
    pack.rename(relocated)
    overrides = tmp_path / "roots.json"
    overrides.write_text(json.dumps({"version": 1, "roots": {str(pack): str(relocated)}}), encoding="utf-8")
    for family, module, prefix in (("fixed", "tools.strategy_lab.fixed", "fixed"), ("is_oos", "tools.strategy_lab.generate", "is")):
        out = tmp_path / (prefix + "-on"); before = snapshot(out)
        args = [module, *(["run"] if family == "fixed" else []), requests[family, "on"],
            "--data-root", relocated, "--output-dir", out, "--filter-roots", overrides]
        invoke(*args)
        assert snapshot(out) == before
    relocated.rename(tmp_path / "hidden-pack")
    for state in ("off", "on"):
        out = tmp_path / ("fixed-"+state); view = tmp_path / ("report-"+state)
        before = snapshot(out)
        invoke("tools.strategy_lab.fixed", "report", "--dataset", out, "--output-dir", view)
        assert snapshot(out) == before
        for html in view.rglob("*.html"):
            content = html.read_text(encoding="utf-8")
            assert "entry_filter_state" in content and "entry_filter_key" in content
            for href in re.findall('href="([^"]+)"', content):
                assert (html.parent / unquote(unescape(href))).is_file()
        invoke("tools.strategy_lab.analysis.cli", "analyze", "--dataset", tmp_path / ("is-"+state),
            "--scope", "research", "--output", tmp_path / ("analysis-"+state))
    invoke("tools.strategy_lab.analysis.cli", "allocate", "--dataset", "off="+str(tmp_path / "is-off"),
        "--dataset", "on="+str(tmp_path / "is-on"), "--scope", "research", "--rule", "primary_profit",
        "--output", tmp_path / "allocation")
    invoke("tools.strategy_lab.compare_filters", "--baseline", tmp_path / "fixed-off", "--variant", tmp_path / "fixed-on",
        "--output-dir", tmp_path / "comparison")
    with (tmp_path / "comparison/pairs.csv").open(newline="", encoding="utf-8") as f:
        row, = csv.DictReader(f)
    assert float(row["delta_total_trades"]) == float(row["on_total_trades"]) - float(row["off_total_trades"])
    (tmp_path / "cli-readback.json").write_text(json.dumps({"calls": calls, "pair": row,
        "source_hidden": True, "all_report_links_valid": True}, indent=2), encoding="utf-8")
