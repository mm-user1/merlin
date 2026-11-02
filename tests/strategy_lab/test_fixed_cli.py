"""Real bounded command-line publication, readback and core refusal."""
import json
import os
from pathlib import Path
import subprocess
import sys

from tools.strategy_lab.config import REPO_ROOT
from tools.strategy_lab.fixed.storage import open_dataset
from _fixed_helpers import request, save_request, snapshot


def cli(*args, success=True):
    env = os.environ.copy();env.pop("PYTHONPATH", None)
    result = subprocess.run([sys.executable,"-B","-m",*map(str,args)], cwd=REPO_ROOT, env=env,
        capture_output=True,text=True,timeout=120)
    assert (result.returncode == 0) == success, result.stdout+result.stderr
    return result


def test_bounded_cli_series_and_offline_readback(tmp_path):
    pack=tmp_path/"pack";out=tmp_path/"dataset";view=tmp_path/"report"
    cli("tools.strategy_lab.synthetic_parquet","--output-dir",pack,"--symbols","AAA","BBB","--end","2024-02-07")
    path=REPO_ROOT/"tools/strategy_lab/runspecs/s07_fixed_example.json"
    validated=cli("tools.strategy_lab.fixed","validate",path)
    assert '"expected_run_count": 16' in validated.stdout and '"source_admission": "requires_preflight"' in validated.stdout
    cli("tools.strategy_lab.fixed","run",path,"--data-root",pack,"--output-dir",out,"--preflight-only")
    assert not (out/"manifest.json").exists()
    cli("tools.strategy_lab.fixed","report","--dataset",out,"--output-dir",tmp_path/"partial","--allow-partial")
    cli("tools.strategy_lab.fixed","run",path,"--data-root",pack,"--output-dir",out,"--resume")
    assert len(open_dataset(out)["completed"]) == 16
    before=snapshot(out)
    cli("tools.strategy_lab.fixed","run",path,"--data-root",pack,"--output-dir",out)
    assert snapshot(out)==before
    pack.rename(tmp_path/"hidden-pack")
    cli("tools.strategy_lab.fixed","report","--dataset",out,"--output-dir",view)
    assert snapshot(out)==before and len(list(view.glob("accounts/*/report.html")))==16
    # Follow every emitted relative HTML link without a server or source pack.
    from html import unescape
    from urllib.parse import unquote
    import re
    for html in view.rglob("*.html"):
        for href in re.findall('href="([^"]+)"',html.read_text()):
            assert (html.parent/unquote(unescape(href))).is_file()


def test_bounded_real_core_failure_cli(tmp_path):
    pack=tmp_path/"pack";out=tmp_path/"dataset"
    cli("tools.strategy_lab.synthetic_parquet","--output-dir",pack,"--end","2024-02-07")
    raw=request();g=raw["generation"]
    g["parameter_sets"].append({"name":"unrepresentable_quantity","params":{"riskPerTrade":1e308}})
    g["parameter_sets"].append({"name":"after_failure","params":{"riskPerTrade":0.5}})
    g["expected_run_count"]=3
    path=save_request(tmp_path,raw)
    failure=cli("tools.strategy_lab.fixed","run",path,"--data-root",pack,"--output-dir",out,success=False)
    assert "quantity" in failure.stderr.lower()
    m=open_dataset(out,allow_partial=True)
    assert len(m["completed"])==1 and m["failure"]
    import csv
    assert [r["status"] for r in csv.DictReader((out/"series-index.csv").open())] == ["complete","failed","pending"]
    before=snapshot(out/"accounts")
    cli("tools.strategy_lab.fixed","run",path,"--data-root",pack,"--output-dir",out,"--resume",success=False)
    assert snapshot(out/"accounts")==before
