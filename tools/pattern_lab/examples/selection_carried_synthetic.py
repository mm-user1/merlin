"""Seven-month, six-target fixed/carried auxiliary selection; synthetic data only."""
from pathlib import Path
import argparse
import json
import sys
import zipfile
import time
if __package__ in (None,''):sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
from tools.pattern_lab.examples.selection_synthetic import build_inputs,selection_request,MONTHS
from tools.pattern_lab.examples.selection_auxiliary_scorer import SCORER
from tools.pattern_lab.account_study import run_account_study,export_account_bundle
from tools.pattern_lab.account_study.common import fresh_directory,file_hash,write_json
from tools.pattern_lab.account_study.request import bounds
from tools.pattern_lab.auxiliary import publish_snapshot
from tools.pattern_lab.auxiliary.model import TYPES
from tools.pattern_lab.selection import run_selection,verify_selection,export_selection_bundle
from tools.pattern_lab.selection.request import month_bounds


def build_snapshots(parent,output_root):
    start,_,end=bounds(parent.study['request']);values=[];sources={}
    for j,ticker in enumerate(parent.study['request']['instruments']):
        sid='oi'+str(j);symbol=ticker.split('_',1)[1];t=TYPES['openInterest']
        series=dict(seriesId=sid,provider='bybit',providerVersion='v5',venue='BYBIT',contract=symbol,
            market='linearUSDTPerpetual',baseUnit=symbol[:-4],baseMultiplier=1,interval='4h',type=t.document(),
            units={f:symbol[:-4] for f in t.fields})
        rows=[]
        for stamp in range(start,end,14400000):
            # Rotating slopes create changing cutoff membership, without future prices.
            month=next((n for n,m in enumerate(MONTHS) if month_bounds(m)[0]<=stamp<month_bounds(m)[1]),0)
            slope=(j+2*month)%6+1;v=float(128+slope*((stamp-start)//14400000%48))
            rows.append(dict(observedAtMs=stamp,singleOpenInterest=v,openInterest=2*v))
        values.append(dict(series=series,rows=rows,coverage=[[start,end]]));sources[ticker]=series
    t=TYPES['settledFunding'];funding=dict(seriesId='funding',provider='bybit',providerVersion='v5',venue='BYBIT',contract='BTCUSDT',
        market='linearUSDTPerpetual',baseUnit='BTC',baseMultiplier=1,interval=None,type=t.document(),units=dict(fundingRate='ratePerSettlement'))
    values.append(dict(series=funding,rows=[dict(observedAtMs=stamp,fundingRate=(-1 if n%2 else 1)*.000125)
        for n,stamp in enumerate(range(start,end,13*3600000))],coverage=[[start,end]]))
    snapshot=publish_snapshot(values,output_root=output_root,provenance=dict(origin='selection synthetic, no network'))
    def source(s,field):return dict(**{k:s[k] for k in ('seriesId','provider','venue','contract','interval','baseUnit','baseMultiplier')},
        typeId=s['type']['typeId'],field=field,unit=s['units'][field])
    return snapshot,[dict(inputId=name,snapshotId='aux',expectedSnapshotContentId=snapshot.contentId,coveragePolicy='requireComplete',sources=mapping)
        for name,mapping in [('oi',{k:source(v,'singleOpenInterest') for k,v in sources.items()}),('funding',{'*':source(funding,'fundingRate')})]]


def request(parent,bindings,mode):
    doc=selection_request(parent)
    doc.update(schema='pattern-lab.selection-request.v2',selectionId='synthetic-'+mode,capitalMode=mode,
        initialCapital=10000. if mode=='carried' else None,auxiliaryInputs=bindings)
    doc['randomControl']['replicates']=4
    doc['selectors']=[doc['selectors'][0],dict(selectorId='oi',scorerId=SCORER.scorerId,version='1',parameters={})]
    return doc


def run(output_root,*,workers=1):
    began=time.perf_counter();root=fresh_directory(output_root)
    raw=build_inputs(root/'pack');write_json(root/'parent-request.json',raw)
    parent=run_account_study(raw,data_root=root/'pack',output_root=root/'parent',workers=workers)
    snapshot,bindings=build_snapshots(parent,root/'auxiliary');scorers={SCORER.scorerId:SCORER}
    bundles={};checks={}
    for mode in ('fixedReference','carried'):
        doc=request(parent,bindings,mode);write_json(root/(mode+'-request.json'),doc)
        selected=run_selection(doc,parent_root=parent.root,output_root=root/mode,scorers=scorers,auxiliary_roots={'aux':snapshot.root},workers=workers)
        checks[mode]=verify_selection(selected.root,parent_root=parent.root,scorers=scorers,auxiliary_roots={'aux':snapshot.root},workers=workers,require_scorers=True)
        bundles[mode]=export_selection_bundle(selected.root,zip_path=root/(mode+'.zip'))
    bundles['parent']=export_account_bundle(parent.root,zip_path=root/'monthly-parent.zip')
    archive=root/'auxiliary.zip'
    with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED) as z:
        for p in sorted(snapshot.root.rglob('*')):
            if p.is_file():z.write(p,p.relative_to(snapshot.root).as_posix())
    bundles['auxiliary']=dict(path=str(archive),bytes=archive.stat().st_size,sha256=file_hash(archive))
    result=dict(bundles=bundles,verification=checks,elapsedSeconds=time.perf_counter()-began)
    write_json(root/'example-evidence.json',result);return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output-root',type=Path,required=True)
    parser.add_argument('--workers',type=int,choices=range(1,7),default=1)
    args=parser.parse_args();print(json.dumps(run(args.output_root,workers=args.workers),indent=2))
