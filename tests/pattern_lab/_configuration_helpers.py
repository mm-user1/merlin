"""Small deterministic configuration studies; no market inputs or work-directory imports."""
from copy import deepcopy

from data_provider.manifest import format_epoch_ms
from ._account_study_helpers import build_pack, request as account_request, STEP
from ._helpers import ANCHOR_MS


def request(instruments,*,filtered=True,multi_tf=True):
    native=account_request(instruments,filtered=filtered,slots=240)
    allocations=deepcopy(native['allocations'])
    if multi_tf:
        extra=deepcopy(allocations[0]);extra.update(allocationId='hour',timeframeMinutes=60);allocations.append(extra)
    utc=lambda n:format_epoch_ms(ANCHOR_MS+n*STEP)
    stages=[]
    for stage,left,right in (('IS',16,80),('HO1',80,160),('HO2',160,240)):
        stages.append(dict(stageId=stage,startUtc=utc(left),endUtc=utc(right),preparationStartUtc=utc(0),
            protocol=dict(schema_version=1,protocol_id='configuration-'+stage,
                development=dict(start_utc=utc(left),end_utc=utc(right)),
                reserved=dict(start_utc=utc(right),end_utc=utc(300)),earliest_warmup_start_utc=utc(0))))
    return dict(schema='pattern-lab.configuration-study-request.v1',studyId='synthetic-configurations',
        instruments=instruments,sources=native['sources'],allocations=allocations,stages=stages)


def inputs(root,*,count=2,gaps=True,filtered=True,multi_tf=True):
    return request(build_pack(root,count=count,gaps=gaps,slots=240),filtered=filtered,multi_tf=multi_tf)
