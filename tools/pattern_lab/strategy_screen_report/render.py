"""Verified offline assembly and owned, receipt-last publication."""
import base64
import hashlib
import os
from pathlib import Path
import stat
import tempfile

from .. import PatternLabDataError
from ..accounts.contracts import require
from ..strategy_screen import load_strategy_screen, verify_strategy_screen
from .model import canonical, presentation_options, view_bytes
from .payload import build_view

VERSION = 'strategy-screen-report.1'
FAILURE = 'Strategy Screen report generation failed; no completed report was published.'
INTERRUPTED = 'Strategy Screen report generation was interrupted; no completed report was published.'
MAX_HTML = 64 * 1024 * 1024
PACKAGE = Path(__file__).resolve().parent
REPO = PACKAGE.parents[2]
# Direct verification/mapping dependencies, separate from native historical implementation.
DEPENDENCIES = (
    'tools/pattern_lab/__init__.py', 'tools/pattern_lab/strategy_screen/__init__.py',
    'tools/pattern_lab/strategy_screen/artifacts.py', 'tools/pattern_lab/strategy_screen/facts.py',
    'tools/pattern_lab/strategy_screen/analytics.py', 'tools/pattern_lab/strategy_screen/request.py',
    'tools/pattern_lab/configuration_study/codec.py', 'tools/pattern_lab/configuration_study/validation.py',
    'tools/pattern_lab/configuration_study/metrics.py', 'tools/pattern_lab/configuration_study/request.py',
    'tools/pattern_lab/configuration_study/curves.py', 'tools/pattern_lab/account_study/codec.py',
    'tools/pattern_lab/account_study/metrics.py', 'tools/pattern_lab/study/bracket_rules.py',
    'src/data_provider/data.py', 'src/data_provider/manifest.py',
    'tools/pattern_lab/account_study/common.py', 'tools/pattern_lab/account_study/containers.py',
    'tools/pattern_lab/account_study/request.py', 'tools/pattern_lab/accounts/contracts.py',
    'tools/pattern_lab/study/contracts.py',
)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def file_hash(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def sources():
    names = set(DEPENDENCIES)
    names.update(p.relative_to(REPO).as_posix() for p in PACKAGE.iterdir() if p.suffix in ('.py', '.js', '.css', '.html'))
    return {name: sha((REPO/name).read_bytes().replace(b'\r\n', b'\n')) for name in sorted(names)}


def safe_path(value):
    path = Path(value).expanduser().absolute()
    for part in reversed((path, *path.parents)):
        try:
            info = part.lstat()
        except FileNotFoundError:
            continue
        require(not stat.S_ISLNK(info.st_mode) and not getattr(info, 'st_file_attributes', 0) & 0x400, 'linked path')
    return path.resolve()


def output_path(value, *inputs):
    path = safe_path(value)
    for source in (*inputs, REPO/'src', REPO/'data', REPO/'tests'):
        require(not path.is_relative_to(source) and not source.is_relative_to(path), 'overlapping output')
    require(not path.exists() or path.is_dir() and not any(path.iterdir()), 'new or empty output required')
    for parent in path.parents:
        names = set()
        for name in ('completion.json', 'study.json', 'selection.json', 'status.json', 'report.json', 'analysis.json', 'receipt.json'):
            try:
                (parent/name).lstat()
            except FileNotFoundError:
                continue
            names.add(name)
        require(not names.intersection(('completion.json', 'study.json', 'selection.json', 'status.json', 'report.json'))
                and not {'analysis.json', 'receipt.json'} <= names, 'owned artifact ancestor')
    return path


def assemble_html(raw):
    def asset(name):
        return (PACKAGE/name).read_text(encoding='utf-8').replace('\r\n', '\n')
    js = '\n'.join(asset(n) for n in ('details.js', 'linked-chart.js', 'screen.js'))
    token = base64.b64encode(hashlib.sha256(js.encode()).digest()).decode()
    csp = f"default-src 'none'; script-src 'sha256-{token}'; style-src 'unsafe-inline'; img-src data:; connect-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'"
    template = asset('template.html').replace('__CSS__', asset('screen.css')).replace('__JS__', js).replace('__CSP__', csp)
    # Split once: labels containing template tokens cannot trigger further substitution.
    before, after = template.split('__DATA__')
    html = before.encode() + raw + after.encode()
    require(len(html) <= MAX_HTML and html.count(b'<script id="screen-data"') == 1, 'HTML bound/shape')
    return html


def publish_file(temporary, destination, owned):
    with destination.open('xb'):
        pass
    owned.append(destination)
    os.replace(temporary, destination)


def publish(output, files):
    """All content already validated; only task-owned files/directories are cleaned."""
    directories = []; owned = []; temporary = []
    try:
        missing = []; current = output
        while not current.exists():
            missing.append(current); current = current.parent
        for directory in reversed(missing):
            directory.mkdir(); directories.append(directory)
        require(output.is_dir() and not any(output.iterdir()), 'output changed before publication')
        for name, raw in files:
            fd, path = tempfile.mkstemp(prefix='.strategy-screen-', dir=output)
            path = Path(path); temporary.append(path)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw)
            require(path.read_bytes() == raw, 'temporary output validation')
            publish_file(path, output/name, owned)
        # The receipt is last. No fallible cleanup/diagnostics after this commit point.
        return output/'report.html'
    except BaseException:
        # A wrapper can raise after the successful final replace. The owned,
        # exact receipt is the completion point, not the wrapper's return.
        if output/'report.json' in owned:
            try:
                if (output/'report.json').read_bytes() == files[-1][1]:
                    return output/'report.html'
            except OSError:
                pass
        for path in reversed(owned + temporary):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        for directory in reversed(directories):
            try:
                directory.rmdir()
            except OSError:
                pass
        raise


def _render(run_root, output_root, presentation, export_view_data):
    require(type(export_view_data) is bool, 'export flag')
    root = safe_path(run_root)
    option_path = safe_path(presentation) if isinstance(presentation, (str, Path)) else None
    output = output_path(output_root, root, *([option_path] if option_path else []))
    completion = file_hash(root/'completion.json')
    verification = verify_strategy_screen(root)
    reader = load_strategy_screen(root)
    metadata = reader.metadata
    require(verification['ok'] is True and verification['artifactIdentity'] == metadata['artifactIdentity']
            and verification['counts'] == metadata['counts'], 'verification/load identity mismatch')
    options = presentation_options(option_path if option_path else presentation, metadata)
    view = build_view(reader, options)
    raw = view_bytes(view); html = assemble_html(raw)
    receipt = dict(schema='pattern-lab.strategy-screen-report.v1', artifactIdentity=metadata['artifactIdentity'],
        completionSha256=completion, nativeSchema=metadata['schema'], analyticsVersion='strategy-screen-analysis.1',
        nativeImplementation=metadata['implementation'], synthetic=metadata['synthetic'], counts=metadata['counts'],
        verificationLevel='compact', fullExecutionRetained=False, limitation=metadata['limitation'],
        generator=dict(version=VERSION, sources=sources()), presentationSha256=sha(canonical(options).encode()),
        view=dict(schema=view['schema'], sha256=sha(raw)), html=dict(bytes=len(html), sha256=sha(html)))
    files = [('report.html', html)]
    if export_view_data:
        exported = raw+b'\n'; files.append(('view-data.json', exported))
        receipt['export'] = dict(bytes=len(exported), sha256=sha(exported))
    files.append(('report.json', (canonical(receipt)+'\n').encode()))
    require(file_hash(root/'completion.json') == completion, 'source completion changed')
    return publish(output, files)


def render_strategy_screen_report(run_root, *, output_root, presentation=None, export_view_data=False):
    try:
        return _render(run_root, output_root, presentation, export_view_data)
    except KeyboardInterrupt:
        raise
    except Exception:
        pass
    # Outside the private handler: neither __cause__ nor __context__ leaks details.
    raise PatternLabDataError(FAILURE)
