"""Small native three-stage auxiliary study; synthetic inputs only."""
from copy import deepcopy
from pathlib import Path
import argparse
import json
import sys

if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[3]))

from tools.pattern_lab.examples.account_auxiliary_synthetic import build_inputs as native_inputs, ORIGIN, STEP, SLOTS
from data_provider.manifest import format_epoch_ms
from tools.pattern_lab.configuration_study import run_configuration_study, export_public_results


def build_inputs(pack_root,auxiliary_root,*,targets=2):
    native=native_inputs(pack_root,auxiliary_root,targets=targets)
    doc={k:deepcopy(native[k]) for k in ('studyId','instruments','sources','allocations','auxiliaryInputs')}
    # Cover both native exit modes and enough same-scope jobs for six workers.
    trailing=deepcopy(doc['allocations'][0]);trailing['allocationId']='trailing'
    trailing['policy'].update(mode='signalManaged',rewardRisk=None,stopUpdates=True)
    doc['allocations'].append(trailing)
    utc=lambda i:format_epoch_ms(ORIGIN+i*STEP)
    doc.update(schema='pattern-lab.configuration-study-request.v1',stages=[])
    for name,left,right in (('IS',192,864),('HO1',864,1584),('HO2',1584,SLOTS)):
        doc['stages'].append(dict(stageId=name,startUtc=utc(left),endUtc=utc(right),preparationStartUtc=utc(0),
            protocol=dict(schema_version=1,protocol_id='configuration-'+name,
                development=dict(start_utc=utc(left),end_utc=utc(right)),
                reserved=dict(start_utc=utc(right),end_utc=utc(SLOTS+576)),earliest_warmup_start_utc=utc(0))))
    return doc


def run(root,workers=1):
    root=Path(root);root.mkdir(parents=True,exist_ok=False)
    request=build_inputs(root/'pack',root/'auxiliary')
    owner=run_configuration_study(request,data_root=root/'pack',auxiliary_roots={'aux':root/'auxiliary'},
        output_root=root/'owner',workers=workers)
    public=export_public_results(owner.root,output_root=root/'public')
    return dict(owner=str(owner.root),public=str(public.root),counts=owner.metadata['counts'])


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root',required=True,type=Path)
    parser.add_argument('--workers',type=int,choices=range(1,7),default=1)
    args=parser.parse_args();print(json.dumps(run(args.output_root,args.workers),indent=2))
