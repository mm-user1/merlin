"""Experimental dimensionless OI-change ranking; synthetic demonstration only."""
from pathlib import Path
from tools.pattern_lab.selection import Scorer,Score
from tools.pattern_lab.account_study.common import closed


def parameters(p):closed(p,());return {}


def uses(p):
    return [dict(useId='fundingRaw',bindingId='funding',access='raw',publicationLagMs=60000,antecedentMs=0),
        dict(useId='oiChange',bindingId='oi',access='feature',publicationLagMs=60000,antecedentMs=0,
            feature=dict(featureId='oiChange',bindingId='oi',transform='auxiliaryChange',version='1',field='singleOpenInterest',
                publicationLagMs=60000,maxAgeMs=2*86400000,lookbackMinutes=1440))]


def score(view,p):
    for ticker in view.instruments:
        oi=view.auxiliaryFeature(ticker,'oiChange');funding=view.auxiliaryRaw(ticker,'fundingRaw')
        # Rate per settlement; do not interpret an event mean as time weighting.
        components=dict(oiChange=oi['value'],oiKnown=oi['known'],fundingSettlements=len(funding['rows']),
            fundingUnit=funding['series']['units']['fundingRate'],cutoffMs=view.cutoffMs)
        if oi['known']:yield Score(ticker,True,(oi['value'],),None,components)
        else:yield Score(ticker,False,None,'unknownOI',components)


SCORER=Scorer('x.syntheticOI','1',parameters,lambda p:4,('oiChange',),score,
    {'selection_auxiliary_scorer.py':Path(__file__)},auxiliaryUses=uses)
