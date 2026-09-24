"""Small shared synthetic builders for bracket tests; no test-module imports."""
from dataclasses import replace
import numpy as np
import pandas as pd
from tools.pattern_lab.study import bracket, contracts, sequential
from tools.pattern_lab.study.bracket_rules import ExecutionRules
from ._helpers import (ANCHOR_MS, timeframe_bars, instrument_source, publish,
    study_request, study_protocol, fixed_horizon_model, TWO_GREEN_EVERY_BAR, TWO_GREEN_STATE_ENTRY)

RULES = ExecutionRules("OKX", "AAA-USDT-SWAP", "AAA", "USDT", "USDT", "contracts",
                       "1", "1", "0.1", "0.1", 1, None, "0.1", "1")


def rule_entry(venue="OKX"):
    from ._helpers import okx_instrument, bybit_instrument
    raw=okx_instrument()
    fields={k:raw[k] for k in ("instType","ctType","settleCcy","ctVal","ctValCcy","ctMult","lotSz","minSz","tickSz","listTime","state")}
    rules=dict(schema_version=1,source_reference="synthetic",as_of_utc="2026-09-24T00:00:00Z",
        contract_type="linear_perpetual",base_currency="AAA",quote_currency="USDT",settlement_currency="USDT",
        quantity_unit="contracts",quantity_step="1",minimum_quantity="1",price_tick="0.001",minimum_notional=None,
        listed_at_utc="2023-11-14T22:13:20Z",trading_status="live",raw_contract_fields=fields)
    contract="AAA-USDT-SWAP"
    if venue=="BYBIT":
        contract="AAAUSDT"
        fields=dict(contractType="LinearPerpetual",baseCoin="AAA",quoteCoin="USDT",settleCoin="USDT",
            launchTime="1700000000000",status="Trading",qtyStep="0.1",minOrderQty="0.1",minNotionalValue="5",tickSize="0.001")
        rules.update(quantity_unit="AAA",quantity_step="0.1",minimum_quantity="0.1",minimum_notional="5",
                     trading_status="Trading",raw_contract_fields=fields)
    return dict(instrument_id=venue+"_"+contract,venue=venue,contract=contract,instrument_rules=rules)


def evaluate(values, *, signals=(0,), direction="long", rr=1, slots=None, rules=RULES, return_context=False, end_ms=None, **settings):
    values = np.array(values,dtype=np.float64)
    slots = np.arange(len(values)) if slots is None else np.array(slots)
    stamps = ANCHOR_MS+slots*1800000
    series = contracts.BarSeries("OKX_AAA-USDT-SWAP",30,1800000,stamps,stamps//1800000,values,0)
    options=dict(atr_length=1,swing_lookback=1,atr_multiplier=1,initial_capital=1000,risk_pct=2,
                 commission_pct_per_side=0,directions=[direction],reward_risks=[rr])
    options.update(settings)
    normalized = bracket.validate_settings(options,[30])
    instance = dict(model_instance_id="br",model_id="atr_bracket",model_version="1",
        evidence_kind=contracts.SEQUENTIAL_EVIDENCE_KIND,settings=normalized,
        cases={"30":[c.as_json() for c in bracket.resolve_cases(normalized,30)]})
    events = pd.DataFrame([dict(variant_id="v",event_id=str(i),anchor_open_ms=int(stamps[i]),timeframe_minutes=30) for i in signals],
                          columns=["variant_id","event_id","anchor_open_ms","timeframe_minutes"])
    result = bracket.evaluate(series,events,[{"variant_id":"v"}],instance,(int(stamps[0]),end_ms or int(stamps[-1])+1800000),rules)
    kwargs=dict(instrument_id=series.instrument_id,instances=[instance],variants=[{"variant_id":"v"}],
                emissions=events,expected_bars={30:list(map(int,stamps))},rules=rules.semantic())
    sequential.validate(result.tables,**kwargs)
    return (result.tables,kwargs) if return_context else result.tables


def build(root, *, mixed=True):
    values=[(100+i,102+i,99+i,101+i,10+i) for i in range(40)]
    stamps,bars=timeframe_bars(30,values)
    sources=[]
    for venue in ("OKX","BYBIT"):
        entry=rule_entry(venue)
        source=instrument_source(stamps,bars,venue=venue,contract=entry["contract"])
        sources.append(replace(source,instrument_rules=entry["instrument_rules"]))
    publish(root,sources)
    end=ANCHOR_MS+40*1800000
    models=[{"id":"br","model":"atr_bracket","settings":{}}]
    if mixed: models.append(fixed_horizon_model(30,[30,60]))
    request=study_request(protocol=study_protocol(first_ms=ANCHOR_MS,coverage_end_ms=end),
        start_ms=ANCHOR_MS+14*1800000,end_ms=end,warmup_ms=ANCHOR_MS,timeframes=[30],
        hypotheses=[TWO_GREEN_EVERY_BAR,TWO_GREEN_STATE_ENTRY],models=models)
    request.update(schema_version=2,context={},execution={"kind":"development"})
    return request

