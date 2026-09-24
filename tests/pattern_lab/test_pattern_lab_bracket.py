"""Independent bracket arithmetic, source boundaries and frozen admission."""
from dataclasses import replace
from decimal import Decimal, ROUND_FLOOR
import copy
import sys

import numpy as np
import pandas as pd
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.study import bracket, contracts, sequential, spec, validation
from tools.pattern_lab.study.bracket_rules import ExecutionRules, normalize_rules
from ._helpers import ANCHOR_MS, study_request, study_protocol, TWO_GREEN_EVERY_BAR


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


@pytest.mark.parametrize("venue",["OKX","BYBIT"])
def test_quantity_rules_units_and_semantic_identity(venue):
    entry=rule_entry(venue)
    rules=normalize_rules(entry)
    assert rules.base_step==rules.base_minimum=="0.1" and rules.minimum_lots==1
    assert rules.quantity_unit==("contracts" if venue=="OKX" else "AAA")
    assert rules.enforce_minimum_notional==(venue=="BYBIT")
    changed=copy.deepcopy(entry)
    changed["instrument_rules"]["as_of_utc"]="2026-09-25T00:00:00Z"
    assert normalize_rules(changed)==rules
    changed["instrument_rules"]["quantity_step"] += "0" if "." in rules.quantity_step else ".0"
    assert normalize_rules(changed)==rules


@pytest.mark.parametrize("multiplier",[None,"2","NaN","0"])
def test_unsupported_multiplier_refused_even_in_opaque_archival_rules(multiplier):
    entry=rule_entry()
    entry["instrument_rules"]["raw_contract_fields"]["ctMult"]=multiplier
    with pytest.raises(PatternLabDataError,match="ctMult"): normalize_rules(entry)


def evaluate(values, *, signals=(0,), direction="long", rr=1, slots=None, **settings):
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
    result = bracket.evaluate(series,events,[{"variant_id":"v"}],instance,(int(stamps[0]),int(stamps[-1])+1800000),RULES)
    sequential.validate(result.tables,instrument_id=series.instrument_id,instances=[instance],variants=[{"variant_id":"v"}],
                        emissions=events,expected_bars={30:list(map(int,stamps))},rules=RULES.semantic())
    return result.tables


@pytest.mark.parametrize("direction",["long","short"])
@pytest.mark.parametrize("rr",[1,2,3])
def test_hand_computed_signal_close_rr_and_contract_units(direction,rr):
    # Signal TR=2; inclusive swing -> long stop=97, short stop=103; d=3.
    target=100+(1 if direction=="long" else -1)*rr*3
    values=[[100,101,99,100,10], [100,max(101,target),min(99,target),target,11]]
    tables=evaluate(values,direction=direction,rr=rr)
    trade=tables["trades"].iloc[0]
    # Off-boundary independent decimal oracle, distinct from legacy 23/10/.1.
    lots=int((Decimal(20)/Decimal(3)/Decimal('.1')).to_integral_value(rounding=ROUND_FLOOR))
    assert trade.lots==lots==66
    assert trade.quantity==6.6 or trade.quantity==pytest.approx(6.6,abs=1e-14)
    assert Decimal(trade.instrument_quantity)==66
    assert trade.entry_price==100 and trade.exit_price==target
    assert trade.net_pnl==pytest.approx(19.8*rr)
    assert trade.net_r==pytest.approx(rr)
    assert tables["path"].iloc[-1].balance==pytest.approx(1000+19.8*rr)


def test_gap_resets_indicators_and_carries_capital():
    tables=evaluate([[100,101,99,100,1]]*5,signals=(0,1,2,3),slots=[0,1,3,4,5])
    assert tables["attempts"].reason.tolist()==["filled","no_next_bar","filled","occupied"]
    assert tables["trades"].exit_reason.tolist()==["gap_boundary","terminal"]
    assert tables["path"].balance.tolist()==[1000]*5


@pytest.mark.parametrize("length",[1,2,14])
def test_pine_atr_bitwise_segment_parity(length):
    from strategies.s06_r_trend_v02_b2.signals import pine_atr
    rng=np.random.default_rng(712)
    close=100+np.cumsum(rng.normal(size=120))
    high,low=close+rng.uniform(0,2,120),close-rng.uniform(0,2,120)
    for first,end in ((0,120),(0,13),(13,48),(48,120)):
        expected=pine_atr(pd.DataFrame(dict(High=high[first:end],Low=low[first:end],Close=close[first:end])),length)
        actual=bracket.pine_atr(high[first:end],low[first:end],close[first:end],length)
        np.testing.assert_array_equal(actual.view(np.uint64),expected.view(np.uint64))


@pytest.mark.parametrize("settings",[{"atr_length":True},{"max_leverage":float('nan')},
    {"reward_risks":[2,2.]},{"directions":["long","long"]},{"trailing":"ma"},
    {"risk_pct":101},{"commission_pct_per_side":100},{"max_holding_days":False}])
def test_closed_settings(settings):
    with pytest.raises(PatternLabDataError): bracket.validate_settings(settings,[30])


def test_case_canonicalization_and_source_import_boundary(monkeypatch):
    settings=bracket.validate_settings({"reward_risks":[3,1.5,2],"directions":["short","long"]},[30])
    cases=bracket.resolve_cases(settings,30)
    assert [c.case_id for c in cases]==["long_rr1.5","long_rr2","long_rr3","short_rr1.5","short_rr2","short_rr3"]
    changed=bracket.resolve_cases({**settings,"max_leverage":4},30)
    assert [c.case_id for c in cases]==[c.case_id for c in changed] and cases!=changed
    assert bracket.prior_bars(settings)==13
    old=list(sys.path)
    monkeypatch.setitem(sys.modules,"core",type("Foreign",(),{"__file__":"/foreign/core/__init__.py"})())
    with pytest.raises(PatternLabDataError,match="Foreign preloaded core"): bracket.reference_core()
    assert sys.path==old


def test_reserved_descriptor_ownership():
    descriptor=bracket.DESCRIPTOR
    with pytest.raises(PatternLabDataError,match="extensions cannot"):
        contracts.register_model(descriptor)
    with pytest.raises(PatternLabDataError,match="descriptor"):
        contracts.register_model(contracts.ModelDescriptor(descriptor.model_id,descriptor.version,
            descriptor.validate_settings,descriptor.resolve_cases,lambda *args:None,
            evidence_kind=descriptor.evidence_kind),builtin=True)
    with pytest.raises(PatternLabDataError,match="triple"):
        contracts.is_sequential(dict(model_id="custom",model_version="1",evidence_kind=contracts.SEQUENTIAL_EVIDENCE_KIND))


@pytest.mark.parametrize("opening,expected",[(100,"stop"),(100.5,"target"),(99.5,"stop")])
def test_both_ohlc_paths_and_tie_ambiguity(opening,expected):
    tables=evaluate([[100,101,99,100,1],[opening,104,96,100,1]],signals=(0,))
    t=tables["trades"].iloc[0]
    assert t.exit_reason==expected and t.exit_phase=="intrabar" and t.ambiguous


def test_entry_gap_two_fees_and_negative_capital_not_reset():
    tables=evaluate([[100,101,99,100,1],[90,91,89,90,1]],commission_pct_per_side=.05)
    t=tables["trades"].iloc[0]
    assert t.entry_price==t.exit_price==90 and t.net_pnl==pytest.approx(-.594)
    assert not t.ambiguous
    # A filled short survives until an adverse gap, losing more than initial capital.
    tables=evaluate([[100,101,99,100,1],[100,101,99,100,1],[1000,1001,999,1000,1],
                     [1000,1001,999,1000,1],[1000,1001,999,1000,1]],direction="short",signals=(0,2,3))
    assert tables["path"].iloc[-1].balance<0
    assert tables["attempts"].reason.tolist()==["filled","reentry_suppressed","nonpositive_capital"]


def test_fill_cap_rejection_allows_a_later_signal_same_bar():
    tables=evaluate([[100,101,99,100,1],[200,201,199,200,1],[200,201,199,200,1],
                     [200,201,199,200,1]],signals=(0,1),max_leverage=1)
    assert tables["attempts"].reason.tolist()==["leverage_cap_exceeded","filled"]
    assert tables["trades"].iloc[0].entry_index==2
    assert tables["path"].iloc[1].balance==1000


@pytest.mark.parametrize("bars,reason,phase",[(195,"expiry","open"),(194,"terminal","close")])
def test_four_day_trigger_next_open_and_boundary_precedence(bars,reason,phase):
    tables=evaluate([[100,101,99,100,1]]*bars)
    t=tables["trades"].iloc[0]
    assert t.exit_reason==reason and t.exit_phase==phase
    assert t.entry_time_ms==ANCHOR_MS+1800000
    assert t.holding_ms==193*1800000


@pytest.mark.parametrize("price",[.00001,50000.])
def test_small_high_price_off_boundary_decimal_quantity(price):
    tables=evaluate([[price,price*1.01,price*.99,price,1],[price,price*1.01,price*.99,price,1]])
    a=tables["attempts"].iloc[0]
    distance=Decimal(str(price))*Decimal('.03')
    lots=int((Decimal(20)/distance/Decimal('.1')).to_integral_value(rounding=ROUND_FLOOR))
    # These fixtures are off lot boundaries; tiny float distances cannot change floor.
    assert a.lots==lots
    assert a.reason==("zero_quantity" if lots==0 else "filled")


def test_gapped_warmup_is_not_contiguous_indicator_availability():
    tables=evaluate([[100,101,99,100,1]]*6,slots=[0,1,2,4,5,6],signals=(0,1,2,3,4),atr_length=3)
    assert tables["attempts"].reason.tolist()==["indicator_unavailable","indicator_unavailable","no_next_bar","indicator_unavailable","indicator_unavailable"]
    assert tables["trades"].empty
    assert list(tables["trades"].columns)==sequential.SCHEMAS["trades"].names
