"""Derived verification requirements; never part of sealed economic summaries."""
from ..account_study.common import json_text,parse_json


def description(metadata):
    doc=metadata['request']
    return dict(
        ordinaryRead=dict(role='Load, import and offline report rebuild',requires='Only the sealed selection bundle'),
        exactParent=dict(role='Exact funded execution verification',pins=parse_json(json_text(doc['parent']))),
        auxiliaryRecomputation=dict(role='Auxiliary scorer recomputation, in addition to the exact parent',
            snapshots=[dict(snapshotId=k,contentId=v) for k,v in sorted({b['snapshotId']:b['expectedSnapshotContentId'] for b in doc['auxiliaryInputs']}.items())],
            experimentalSources=parse_json(json_text(metadata['implementation']['experimental']))),
        supply='Caller supplies root paths and explicitly supplies matching scorer code. No network fallback or recorded-code auto-import. Parent, snapshots and scorer code are not included by selection export.',
        verification='Export and report rebuild do not perform deep execution or scorer verification. auxiliaryRecomputed means at least one nonempty auxiliary selector was actually recomputed and compared; per-selector levels determine completeness.')
