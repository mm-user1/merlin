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

VERSION = 'strategy-screen-report.3'
FAILURE = 'Strategy Screen report generation failed; no completed report was published.'
INTERRUPTED = 'Strategy Screen report generation was interrupted; no completed report was published.'
UNCERTAIN = 'Strategy Screen report publication could not be confirmed; output was preserved for inspection.'


class PublicationUnknown(PatternLabDataError):
    """The final replacement was attempted but its outcome could not be observed."""


class PublicationInterrupted(KeyboardInterrupt):
    """Interrupted final replacement or reconciliation with an unknown outcome."""

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
    'tools/pattern_lab/accounts/preparation.py', 'tools/pattern_lab/study/spec.py',
    'tools/pattern_lab/auxiliary/request.py', 'tools/pattern_lab/auxiliary/features.py',
    'tools/pattern_lab/auxiliary/model.py',
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
    stream = destination.open('xb')
    owned['claims'][destination] = None
    with stream:
        info = os.fstat(stream.fileno())
        owned['claims'][destination] = (info.st_dev, info.st_ino)
    if destination.name == 'report.json':
        owned['receiptAttempted'] = True
    os.replace(temporary, destination)


def publish(output, files):
    """All content already validated; only task-owned files/directories are cleaned."""
    directories = []; owned = {'claims': {}, 'receiptAttempted': False}; temporary = []
    interrupted = False; unknown = False
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
    except (Exception, KeyboardInterrupt) as error:
        interrupted = isinstance(error, KeyboardInterrupt)
        # A wrapper can raise after the successful final replace. The owned,
        # exact receipt is the completion point, not the wrapper's return.
        receipt = output/'report.json'
        if owned['receiptAttempted']:
            unknown = True
            try:
                with receipt.open('rb') as stream:
                    info = os.fstat(stream.fileno())
                    observed = stream.read(len(files[-1][1]) + 1)
                if observed == files[-1][1]:
                    return output/'report.html'
                if observed == b'' and (info.st_dev, info.st_ino) == owned['claims'][receipt]:
                    unknown = False
            except (Exception, KeyboardInterrupt) as diagnostic:
                interrupted |= isinstance(diagnostic, KeyboardInterrupt)
    if not unknown:
        for path in reversed(list(owned['claims']) + temporary):
            try:
                path.unlink(missing_ok=True)
            except (OSError, KeyboardInterrupt) as cleanup:
                interrupted |= isinstance(cleanup, KeyboardInterrupt)
        for directory in reversed(directories):
            try:
                directory.rmdir()
            except (OSError, KeyboardInterrupt) as cleanup:
                interrupted |= isinstance(cleanup, KeyboardInterrupt)
    # No private exception remains active, including diagnostic/cleanup interrupts.
    if unknown:
        raise (PublicationInterrupted if interrupted else PublicationUnknown)(UNCERTAIN)
    raise (KeyboardInterrupt if interrupted else PatternLabDataError)(INTERRUPTED if interrupted else FAILURE)


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
    unknown = False; interrupted = False
    try:
        return _render(run_root, output_root, presentation, export_view_data)
    except PublicationInterrupted:
        unknown = interrupted = True
    except PublicationUnknown:
        unknown = True
    except KeyboardInterrupt:
        interrupted = True
    except Exception:
        pass
    # Outside the private handler: neither __cause__ nor __context__ leaks details.
    if unknown:
        raise (PublicationInterrupted if interrupted else PublicationUnknown)(UNCERTAIN)
    raise (KeyboardInterrupt if interrupted else PatternLabDataError)(INTERRUPTED if interrupted else FAILURE)
