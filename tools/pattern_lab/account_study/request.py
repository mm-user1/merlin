"""One closed normalization path, including declared complete-grid warmup."""
from dataclasses import dataclass, fields, asdict, MISSING
from pathlib import Path

from ..accounts import AccountFeatureRequest, HARecipe, ExecutionPolicy
from ..accounts.contracts import require, identifier, integer, number
from ..accounts.preparation import validate_request
from ..manifest import normalize_timeframe_minutes, to_epoch_ms, format_epoch_ms
from ..study.spec import normalize_protocol, protocol_document, validate_against_protocol
from ..study.contracts import semantic_digest
from .common import closed, read_json, parse_json, json_text

SCHEMA = 'pattern-lab.account-study-request.v1'


def native(cls, value):
    required = [f.name for f in fields(cls) if f.default is MISSING and f.default_factory is MISSING]
    closed(value, required, [f.name for f in fields(cls)])
    return cls(**value)


@dataclass(frozen=True)
class AccountStudyRequest:
    """Immutable canonical JSON; public access returns independent mappings."""
    documentJson: str

    @property
    def document(self):
        return parse_json(self.documentJson)

    @property
    def identity(self):
        doc = self.document
        doc.pop('notes', None)
        doc['protocol'].pop('notes', None)
        return semantic_digest(doc)


def bounds(doc):
    return (to_epoch_ms(doc['preparationStartUtc'], 'preparationStartUtc'),
            min(to_epoch_ms(w['startUtc'], 'startUtc') for w in doc['windows']),
            max(to_epoch_ms(w['endUtc'], 'endUtc') for w in doc['windows']))


def warmup_requirements(doc, allocation):
    origin, start, _ = bounds(doc)
    steps = {'target': allocation['timeframeMinutes']*60000,
             **{s['inputId']: s['timeframeMinutes']*60000 for s in doc['sources']}}
    required = {'target': 0}
    for f in allocation['features']:
        p = f['parameters']
        intrinsic = p['length']-1 if f['featureId'] == 'atr' else (
            p['confirmationBars']-1 if f['featureId'] == 'candleDirection' else 0)
        required[f['inputId']] = max(required.get(f['inputId'], 0), f['priorBars'], intrinsic)
    recipe, policy = allocation['provider']['recipe'], allocation['policy']
    required['target'] = max(required['target'], recipe['entryRunLength']-1,
        recipe['exitRunLength']-1 if policy['mode'] == 'signalManaged' else 0,
        1 if recipe['stopProvider'] == 'haSwing2' else
        recipe['swingBars']-1 if recipe['stopProvider'] == 'realSwingAtr' else 0)
    decision = start + steps['target']
    result = {}
    for alias, count in sorted(required.items()):
        available = (start-origin)//steps[alias] if alias == 'target' else (decision-origin)//steps[alias]-1
        require(available >= count, 'insufficient declared warmup for '+allocation['allocationId']+'/'+alias)
        result[alias] = dict(requiredPriorSlots=count, availablePriorSlots=available, stepMs=steps[alias])
    return result


def normalize_request(value):
    base = Path.cwd()
    if isinstance(value, AccountStudyRequest):
        value = value.document
    elif isinstance(value, (str, Path)):
        path = Path(value).expanduser().resolve()
        value, base = read_json(path), path.parent
    doc = parse_json(json_text(value))  # owned copy; rejects NaN and non-JSON values
    closed(doc, ('schema', 'studyId', 'protocol', 'preparationStartUtc', 'instruments', 'sources', 'windows', 'allocations'), ('notes',))
    require(doc['schema'] == SCHEMA, 'account study request schema')
    identifier(doc['studyId'])
    require(doc.get('notes') is None or isinstance(doc['notes'], str), 'notes')
    protocol = doc['protocol']
    if isinstance(protocol, str):
        protocol = read_json(base/protocol)
    protocol = normalize_protocol(protocol, source='account protocol')
    doc['protocol'] = protocol_document(protocol)
    doc['preparationStartUtc'] = format_epoch_ms(to_epoch_ms(doc['preparationStartUtc'], 'preparationStartUtc'))
    for key in ('instruments', 'sources', 'windows', 'allocations'):
        require(type(doc[key]) is list and (doc[key] or key == 'sources'), key+' must be an explicit list')
    for item in doc['instruments']:
        identifier(item)
        require(item != 'target', 'target is a source token, not a pack instrument')
    require(len(set(doc['instruments'])) == len(doc['instruments']), 'duplicate targets')
    doc['instruments'].sort()
    aliases, equivalents = set(), set()
    for source in doc['sources']:
        closed(source, ('inputId', 'instrumentId', 'timeframeMinutes'))
        identifier(source['inputId']); identifier(source['instrumentId'])
        require(source['inputId'] != 'target' and source['inputId'] not in aliases, 'duplicate/reserved source alias')
        source['timeframeMinutes'] = normalize_timeframe_minutes(source['timeframeMinutes'])
        pair = (source['instrumentId'], source['timeframeMinutes'])
        require(pair not in equivalents, 'duplicate equivalent source')
        equivalents.add(pair); aliases.add(source['inputId'])
    for key, id_key in (('windows', 'windowId'), ('allocations', 'allocationId')):
        identifiers = []
        for item in doc[key]:
            require(type(item) is dict, key+' item')
            identifier(item.get(id_key)); identifiers.append(item[id_key])
        require(len(set(identifiers)) == len(identifiers), 'duplicate '+id_key)
        doc[key].sort(key=lambda item: item[id_key])
    for window in doc['windows']:
        closed(window, ('windowId', 'startUtc', 'endUtc'))
        for k in ('startUtc', 'endUtc'):
            window[k] = format_epoch_ms(to_epoch_ms(window[k], k))
        require(window['startUtc'] < window['endUtc'], 'window start must precede end')
    origin, start, end = bounds(doc)
    require(origin <= start, 'preparation origin after research start')
    validate_against_protocol(protocol, study_start_ms=start, study_end_ms=end, warmup_start_ms=origin)
    used = set()
    for a in doc['allocations']:
        closed(a, ('allocationId', 'timeframeMinutes', 'features', 'provider', 'entryFilter', 'policy'))
        a['timeframeMinutes'] = normalize_timeframe_minutes(a['timeframeMinutes'])
        policy = native(ExecutionPolicy, a['policy']); a['policy'] = asdict(policy)
        p = closed(a['provider'], ('providerId', 'version', 'recipe'))
        require((p['providerId'], p['version']) == ('haRun', '1'), 'unsupported account provider/version')
        recipe = native(HARecipe, p['recipe'])
        for k in ('entryRunLength', 'exitRunLength', 'swingBars'):
            integer(getattr(recipe, k), k, 1)
        number(recipe.atrMultiplier, 'atrMultiplier', inclusive=True)
        require(recipe.stopProvider in ('haLast', 'haSwing2', 'realSwingAtr'), 'stop provider')
        p['recipe'] = asdict(recipe)
        require(type(a['features']) is list and a['features'], 'features')
        features = {}
        for raw in a['features']:
            f = native(AccountFeatureRequest, raw)
            validate_request(f, {'target': None, **dict.fromkeys(aliases)})
            require(f.requestId not in features, 'duplicate feature request')
            features[f.requestId] = f
            if f.inputId != 'target': used.add(f.inputId)
        ha = features.get(recipe.haRequestId)
        require(ha is not None and ha.featureId == 'ha' and ha.inputId == 'target', 'HA dependency')
        if recipe.stopProvider == 'realSwingAtr':
            atr = features.get(recipe.atrRequestId)
            require(atr is not None and atr.featureId == 'atr' and atr.inputId == 'target', 'ATR dependency')
        else:
            require(recipe.atrRequestId is None, 'unused ATR dependency')
        f = a['entryFilter']
        if f is not None:
            closed(f, ('providerId', 'version', 'requestId'))
            require((f['providerId'], f['version']) == ('candleDirection', '1') and
                    f['requestId'] in features and features[f['requestId']].featureId == 'candleDirection',
                    'filter provider/dependency')
        a['features'] = [asdict(features[k]) for k in sorted(features)]
        warmup_requirements(doc, a)
        steps = [a['timeframeMinutes']*60000] + [s['timeframeMinutes']*60000 for s in doc['sources']
            if s['inputId'] in {f.inputId for f in features.values()}]
        endpoints = [origin, start, end] + [to_epoch_ms(w[k], k) for w in doc['windows'] for k in ('startUtc', 'endUtc')]
        require(all(t % step == 0 for t in endpoints for step in steps), 'unaligned source/target endpoint')
    require(used == aliases, 'unused source binding')
    doc['sources'].sort(key=lambda s: s['inputId'])
    return AccountStudyRequest(json_text(doc))


def planned_family(doc):
    return [dict(instrumentId=i, allocationId=a['allocationId'], windowId=w['windowId'])
            for i in doc['instruments'] for a in doc['allocations'] for w in doc['windows']]
