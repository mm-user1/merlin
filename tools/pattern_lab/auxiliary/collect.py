"""Serial explicit collection through the existing paced/retrying HTTP client."""
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, parse_qsl
import hashlib
import shutil
import re

from .. import exchange_data as ex
from ..accounts.contracts import require, identifier, integer
from ..account_study.common import closed, read_json, write_json, json_text, parse_json
from ..manifest import to_epoch_ms, format_epoch_ms
from .model import TYPES, INTERVALS, normalize_rows, content, spans, covered
from .storage import new_root, load_snapshot, write_series, finish_snapshot

REQUEST_SCHEMA = 'pattern-lab.auxiliary-collection-request.v1'


def normalize_collection(request):
    raw = read_json(request) if isinstance(request, (str, Path)) else request
    doc = parse_json(json_text(raw))
    closed(doc, ('schema', 'provider', 'series'))
    require(doc['schema'] == REQUEST_SCHEMA and doc['provider'] in PROVIDERS, 'collection schema/provider')
    require(type(doc['series']) is list and doc['series'], 'declared collection series')
    ids = set()
    for s in doc['series']:
        closed(s, ('seriesId', 'contract', 'typeId', 'interval', 'startUtc', 'endUtc', 'tailMarginMs', 'baseMultiplier'))
        identifier(s['seriesId'])
        require(isinstance(s['contract'], str) and re.fullmatch('[A-Za-z0-9_.-]+',s['contract']), 'contract')
        from ..account_study.common import relative_name
        relative_name(s['seriesId'])
        require(s['seriesId'] not in ids, 'duplicate series'); ids.add(s['seriesId'])
        require(s['typeId'] in TYPES, 'unknown auxiliary type')
        require(s['interval'] in INTERVALS if TYPES[s['typeId']].kind == 'sampled' else s['interval'] is None, 'explicit sampling interval')
        for k in ('startUtc', 'endUtc'):
            require(isinstance(s[k], str) and (s[k].endswith('Z') or s[k].endswith('+00:00')), 'timezone-qualified UTC required')
            s[k] = format_epoch_ms(to_epoch_ms(s[k], k))
        require(s['startUtc'] < s['endUtc'], 'collection bounds')
        integer(s['tailMarginMs'], 'tailMarginMs', INTERVALS[s['interval']] if s['interval'] else 3600000)
        from ..accounts.contracts import number
        number(s['baseMultiplier'], 'baseMultiplier')
    doc['series'].sort(key=lambda s:s['seriesId'])
    return doc


class BybitProvider:
    def __init__(self): self.adapter = ex.BybitAdapter()

    def server_time_ms(self, client): return self.adapter.server_time_ms(client)

    def fetch(self, request, client):
        r = request; contract = r['contract']
        require(r['typeId'] in ('settledFunding', 'openInterest'), 'Bybit type unsupported')
        metadata = self.adapter.instrument_metadata(client, contract)
        start, end = (to_epoch_ms(r[k], k) for k in ('startUtc', 'endUtc'))
        listed = metadata['listed_at_utc']
        actual_start = max(start, to_epoch_ms(listed, 'listed')) if listed else start
        t = TYPES[r['typeId']]
        series = dict(seriesId=r['seriesId'], provider='bybit', providerVersion='v5', venue='BYBIT',
            contract=contract, market='linearUSDTPerpetual', baseUnit=metadata['base_currency'],
            baseMultiplier=r['baseMultiplier'], interval=r['interval'], type=t.document(),
            units={f:'ratePerSettlement' if r['typeId']=='settledFunding' else metadata['base_currency'] for f in t.fields})
        rows, pages, cursors, seen = [], [], set(), {}
        cursor, upper = '', end-1
        terminal = 'prelisting'
        if actual_start < end:
            for page in range(10000):
                funding = r['typeId'] == 'settledFunding'
                params = dict(category='linear', symbol=contract, startTime=actual_start, endTime=upper, limit=200)
                if not funding:
                    params.update(intervalTime=r['interval'])
                    if cursor: params['cursor'] = cursor
                result = self.adapter._get(client, '/v5/market/funding/history' if funding else '/v5/market/open-interest',
                                           params, 'auxiliary '+r['seriesId'])
                require(result.get('category', 'linear') == 'linear' and result.get('symbol', contract) == contract, 'auxiliary source identity')
                raw = result.get('list')
                require(type(raw) is list and len(raw) <= 200, 'auxiliary page list/size')
                stamps, page_rows = [], []
                for row in raw:
                    require(type(row) is dict and row.get('symbol', contract) == contract, 'row source identity')
                    stamp = ex._epoch_ms(row.get('fundingRateTimestamp' if funding else 'timestamp'), 'auxiliary timestamp')
                    require(actual_start <= stamp < end and stamp <= upper, 'auxiliary response outside bounds')
                    stamps.append(stamp)
                    item = dict(observedAtMs=stamp)
                    for f in t.fields:
                        v = row.get(f)
                        item[f] = None if v is None else ex._float(v, f)
                    page_rows.append(item)
                for item in normalize_rows(series, page_rows):
                    stamp = item['observedAtMs']
                    require(stamp not in seen or seen[stamp] == item, 'conflicting auxiliary observation')
                    if stamp not in seen: rows.append(item)
                    seen[stamp] = item
                require(stamps == sorted(stamps, reverse=True), 'auxiliary page order')
                pages.append(dict(parameters=params, rows=len(raw), oldest=min(stamps) if stamps else None,
                                  newest=max(stamps) if stamps else None))
                require(len(rows) <= 2000000, 'collection row bound')
                if funding:
                    if not stamps: terminal = 'emptyPage'; break
                    next_upper = min(stamps)-1
                    require(next_upper < upper, 'nonprogressing Funding page')
                    upper = next_upper
                    if upper < actual_start: terminal = 'lowerBound'; break
                    # A short nonempty page is not exhaustion.
                else:
                    following = result.get('nextPageCursor', '')
                    require(isinstance(following, str), 'OI cursor type')
                    if not following: terminal = 'cursorExhausted'; break
                    require(following != cursor and following not in cursors and stamps, 'nonprogressing OI cursor')
                    if len(pages) > 1:
                        require(min(stamps) < pages[-2]['oldest'], 'nonprogressing OI page time')
                    cursors.add(following); cursor = following
            else: raise ValueError('auxiliary page limit exceeded')
        coverage = [[actual_start, end]] if actual_start < end else []
        value = content(series, normalize_rows(series, rows), coverage)
        return value, dict(pages=pages, terminalReason=terminal, metadata=metadata,
            prelistingUnavailable=[[start, min(actual_start, end)]] if start < actual_start else [],
            publicationCompleteness='Unmeasured; margin and overlap do not prove complete history')


PROVIDERS = {'bybit': BybitProvider}


def collect_auxiliary(request, *, output_root, previous_root=None, client=None):
    doc = normalize_collection(request)
    root = new_root(output_root, previous_root)
    previous = load_snapshot(previous_root) if previous_root is not None else None
    old_ids = {s['seriesId'] for s in previous.manifest['series']} if previous else set()
    # Static overlap admission precedes any network and output creation.
    for r in doc['series']:
        if r['seriesId'] in old_ids:
            old_meta = read_json(previous.root/'series'/r['seriesId']/'series.json')
            old = old_meta['request']
            require(old is not None, 'incremental collection requires prior collection bounds')
            for k in ('contract', 'typeId', 'interval', 'baseMultiplier'): require(old[k] == r[k], 'incompatible series update '+k)
            start, end, old_start, old_end = (to_epoch_ms(v, 'bounds') for v in
                (r['startUtc'], r['endUtc'], old['startUtc'], old['endUtc']))
            require(start <= max(old_start, old_end-old['tailMarginMs']) and end >= old_end, 'required tail overlap skipped')
    client = client or ex.HttpClient(timeout=20, max_attempts=5, rates={'BYBIT': 2.0})
    transport, attempts = client.transport, []
    def recording(url, timeout):
        response = transport(url, timeout)
        outcome = ex._classify_attempt(response, ex._bybit_business_problem)
        attempts.append(dict(url=urlsplit(url)._replace(query='').geturl(), parameters=dict(parse_qsl(urlsplit(url).query)),
            fetchedAtUtc=client.clock.now_utc().isoformat(), status=response.status,
            bytes=len(response.body.encode('utf-8')), sha256=hashlib.sha256(response.body.encode('utf-8')).hexdigest(),
            outcome=outcome.reason, ok=outcome.ok, retryable=outcome.retryable))
        return response
    client.transport = recording
    provider = PROVIDERS[doc['provider']]()
    try:
        server = provider.server_time_ms(client)
        for r in doc['series']:
            require(to_epoch_ms(r['endUtc'], 'endUtc') <= server-r['tailMarginMs'], 'unsafe explicit collection tail')
        root.mkdir(parents=True)
        entries = []
        for r in doc['series']:
            attempt_start = len(attempts)
            value, receipt = provider.fetch(r, client)
            require(value['series']['seriesId'] == r['seriesId'], 'provider series binding')
            require(value['series']['provider']==doc['provider'] and value['series']['contract']==r['contract'] and
                    value['series']['type']['typeId']==r['typeId'] and value['series']['interval']==r['interval'] and
                    value['series']['baseMultiplier']==r['baseMultiplier'], 'provider request binding')
            late = 0
            merged_request = dict(r)
            if r['seriesId'] in old_ids:
                old = previous.series(r['seriesId'])
                require(old['series'] == value['series'], 'changed series definition/units')
                start, end = (to_epoch_ms(r[k], k) for k in ('startUtc', 'endUtc'))
                def key(row): return (row['observedAtMs'], row.get('eventId'))
                fresh = {key(row):row for row in value['rows']}
                older = {key(row):row for row in old['rows']}
                for k, row in older.items():
                    if start <= row['observedAtMs'] < end:
                        require(k in fresh and fresh[k] == row, 'changed or disappearing historical observation')
                late = sum(k not in older and covered(row['observedAtMs'], old['coverage']) for k,row in fresh.items())
                value = content(value['series'], [*old['rows'], *value['rows']], spans(old['coverage']+value['coverage']))
                old_request = read_json(previous.root/'series'/r['seriesId']/'series.json')['request']
                merged_request['startUtc'] = min(r['startUtc'], old_request['startUtc'])
            receipt.update(attempts=attempts[attempt_start:], lateArrivals=late, actualBounds=[r['startUtc'], r['endUtc']],
                           serverTimeMs=server, tailMarginMs=r['tailMarginMs'])
            entries.append(write_series(root, value, merged_request, receipt))
            del value
        declared = {r['seriesId'] for r in doc['series']}
        for sid in sorted(old_ids-declared):
            shutil.copytree(previous.root/'series'/sid, root/'series'/sid)
            entries.append(next(r for r in previous.manifest['series'] if r['seriesId'] == sid))
        return finish_snapshot(root, entries, dict(request=doc, serverTimeMs=server,
            serverAttempts=attempts[:1], previousProvenanceId=previous.seal['provenanceId'] if previous else None))
    except BaseException as exc:
        if root.exists() and not (root/'completion.json').exists():
            write_json(root/'failure.json', dict(state='failed', errorType=type(exc).__name__, error=str(exc),
                attempts=attempts, finishedUtc=datetime.now(timezone.utc).isoformat()))
        raise
    finally:
        client.transport = transport
