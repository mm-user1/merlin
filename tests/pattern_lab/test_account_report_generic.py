"""The generic harness consumes generated HTML without strict-fixture assumptions."""
from pathlib import Path
import re
import shutil
import subprocess

import pytest

from tools.pattern_lab.account_study import run_account_study
from ._account_study_helpers import build_pack, request


def test_generic_node_report_accepts_plain_fixture_and_refuses_broken_models(tmp_path):
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node unavailable; generic acceptance/corruption gate pending Linux Node review')
    ids = build_pack(tmp_path/'pack', count=1)
    raw = request(ids, filtered=False, multiple=False)
    raw['allocations'][0]['allocationId'] = 'plain'
    raw['windows'] = raw['windows'][:1]
    reader = run_account_study(raw, data_root=tmp_path/'pack', output_root=tmp_path/'study')
    html = (reader.root/'derived/report.html').read_text(encoding='utf-8')
    harness = Path(__file__).with_name('account_report_generic.cjs')
    good = subprocess.run([node, str(harness), str(reader.root/'derived/report.html')],
                          capture_output=True, text=True, timeout=60)
    assert good.returncode == 0, good.stdout + good.stderr
    for label, broken in [('missing', '<html></html>'), ('malformed', '{'), ('empty', '{}'),
                          ('incomplete', '{"request":{},"summary":{}}')]:
        value = broken if label == 'missing' else re.sub(
            r'(<script id="account-data" type="application/json">)[\s\S]*?(</script>)',
            lambda m: m[1] + broken + m[2], html)
        path = tmp_path/(label+'.html'); path.write_text(value, encoding='utf-8')
        result = subprocess.run([node, str(harness), str(path)], capture_output=True, text=True, timeout=60)
        assert result.returncode != 0 and result.stderr, label
