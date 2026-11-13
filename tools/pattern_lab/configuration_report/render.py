"""Deterministic assembly: verified native gzip bytes are copied, never decoded here."""
import base64
import hashlib
import os
from pathlib import Path
import tempfile

from ..accounts.contracts import require
from ..configuration_study import load_configuration_study, normalize_annotations
from ..configuration_study.metrics import month_equivalents
from data_provider.manifest import to_epoch_ms
from .model import canonical, presentation_options

VERSION = 'configuration-report.1'
ASSETS = Path(__file__).parent
SOURCES = ('__init__.py', 'core.js', 'model.py', 'render.py', 'report.css', 'report.js', 'template.html')
MAX_HTML = 256 * 1024 * 1024


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def normalized(name):
    raw = (ASSETS / name).read_bytes().replace(b'\r\n', b'\n')
    raw.decode('utf-8')
    return raw


def file_info(path):
    digest = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        for raw in iter(lambda: stream.read(1024 * 1024), b''):
            size += len(raw)
            digest.update(raw)
    return dict(bytes=size, sha256=digest.hexdigest())


def validate_html(path, expected_blocks):
    """Validate assembly framing and bounds before publication, without inflating data."""
    count = 0
    with path.open('rb') as stream:
        first = stream.readline()
        require(first.startswith(b'<!doctype html>'), 'HTML framing')
        for line in stream:
            count += line.startswith(b'<script type="application/octet-stream"')
        require(line.rstrip() == b'</body></html>', 'HTML closing frame')
    require(count == expected_blocks and path.stat().st_size <= MAX_HTML, 'HTML assembly bounds')


def publish_file(temporary, destination, owned):
    # Exclusive destination creation is portable and never overwrites a concurrent
    # file. The empty claim belongs to this call; replace publishes verified bytes.
    with destination.open('xb'):
        pass
    owned.append(destination)
    try:
        os.replace(temporary, destination)
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


def render_configuration_report(run_root, *, output_root, annotations=None, presentation=None):
    source = Path(run_root).expanduser().resolve()
    output = Path(output_root).expanduser().resolve()
    require(not source.is_relative_to(output) and not output.is_relative_to(source), 'overlapping source/output')
    require(not output.exists() or output.is_dir() and not any(output.iterdir()), 'occupied output')
    study = load_configuration_study(source)
    metadata = study.metadata
    options = presentation_options(presentation, metadata)
    annotation = normalize_annotations(annotations, study) if annotations is not None else dict(
        schema='pattern-lab.configuration-annotations.v1', specificationIdentity=metadata['identities']['specification'], annotations=[])
    generator = dict(version=VERSION, hashNormalization='UTF-8 CRLF-to-LF',
                     sources={name: sha(normalized(name)) for name in SOURCES})
    attribution = dict(sourceCompletionSha256=file_info(source / 'completion.json')['sha256'],
        identities=metadata['identities'], artifactIdentity=metadata['artifactIdentity'], visibility=metadata['visibility'],
        synthetic=metadata['synthetic'], generator=generator, presentationSha256=sha(canonical(options).encode()),
        annotationsSha256=sha(canonical(annotation).encode()), counts=metadata['counts'])
    stages = []
    for stage in metadata['request']['stages']:
        start, end = (to_epoch_ms(stage[key], key) for key in ('startUtc', 'endUtc'))
        stages.append(dict(stageId=stage['stageId'], startMs=start, endMs=end, months=month_equivalents(start, end)))
    index = study.chunk_index
    manifest = dict(schema='pattern-lab.configuration-report-bootstrap.v1', metadata=metadata,
        stages=stages, rows=study.stage_rows, index=index, presentation=options, annotations=annotation, attribution=attribution)
    script = normalized('core.js') + b'\n' + normalized('report.js')
    csp_hash = base64.b64encode(hashlib.sha256(script).digest()).decode()
    template = normalized('template.html').decode().replace('{{CSS}}', normalized('report.css').decode())
    template = template.replace('{{CSP_HASH}}', csp_hash).replace('{{BOOTSTRAP}}', canonical(manifest).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026'))
    before, after = template.split('{{BLOCKS}}')
    after = after.replace('{{JS}}', script.decode())
    created = False
    owned = []
    try:
        if not output.exists():
            output.mkdir(parents=True)
            created = True
        require(not any(output.iterdir()), 'occupied output')
        fd, temporary = tempfile.mkstemp(prefix='.report-', suffix='.tmp', dir=output)
        temp = Path(temporary)
        owned.append(temp)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(before.encode())
            for i, entry in enumerate(index['members']):
                raw = study.raw_member(entry['name'])
                stream.write(f'<script type="application/octet-stream" id="block-{i}">'.encode())
                stream.write(base64.b64encode(raw))
                stream.write(b'</script>\n')
                require(stream.tell() <= MAX_HTML, 'HTML byte bound')
            stream.write(after.encode())
        validate_html(temp, len(index['members']))
        info = file_info(temp)
        receipt = dict(schema='pattern-lab.configuration-report.v1', **attribution, html=info)
        html = output / 'report.html'
        publish_file(temp, html, owned)
        fd, temporary = tempfile.mkstemp(prefix='.receipt-', suffix='.tmp', dir=output)
        temp = Path(temporary)
        owned.append(temp)
        with os.fdopen(fd, 'wb') as stream:
            stream.write((canonical(receipt) + '\n').encode())
        require(file_info(html) == info, 'HTML changed before receipt publication')
        final = output / 'report.json'
        publish_file(temp, final, owned)
        return html
    except BaseException:
        for path in reversed(owned):
            path.unlink(missing_ok=True)
        if created:
            try:
                output.rmdir()
            except OSError:
                pass  # Preserve unrelated/concurrent files, never recursive cleanup.
        raise
