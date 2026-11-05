"""Synthetic pack metadata edits and independent execution witnesses for replay tests."""
from data_provider import manifest


def change_rules(source, *, step=None, minimum=None, notional=..., ct_val=None, base=None, provenance=None):
    document = manifest.read_manifest(source['pack_root'])
    for entry in document['instruments']:
        rules = entry['instrument_rules']; raw = rules['raw_contract_fields']
        bybit = entry['venue'] == 'BYBIT'
        for value, key, field in [(step, 'quantity_step', 'qtyStep' if bybit else 'lotSz'),
                                  (minimum, 'minimum_quantity', 'minOrderQty' if bybit else 'minSz')]:
            if value is not None:
                rules[key] = raw[field] = value
        if notional is not ...:
            rules['minimum_notional'] = notional
            if bybit: raw['minNotionalValue'] = notional
        if ct_val is not None: raw['ctVal'] = ct_val
        if base is not None:
            rules['base_currency'] = raw['baseCoin' if bybit else 'ctValCcy'] = base
            if bybit: rules['quantity_unit'] = base
        if provenance is not None: rules['source_reference'] = provenance
    manifest.write_manifest(source['pack_root'], document)


def assert_current_rules_differ(strategy, frame, params, idx, original, *, admission_rules=None,
                                quantity_rules=None, **options):
    from core.engine_v2.admission import run_strategy
    current = run_strategy(strategy, frame, params, idx, admission_rules=admission_rules,
                           quantity_rules=quantity_rules, **options)
    assert original.trades and current.trades
    assert current.trades != original.trades
    return current
