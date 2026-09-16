#!/usr/bin/env python3
"""Pair file-selection and history controls on identical persisted fixtures."""
import argparse, json, os, random, statistics, subprocess
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--summarize-only',action='store_true',help='summarize completed raw pairs without rerunning executables');
p.add_argument('baseline'); p.add_argument('candidate'); p.add_argument('--pairs',type=int,default=10)
p.add_argument('--cases',default='dense64,sparse64'); p.add_argument('--rtt',type=int,default=0)
p.add_argument('--output',required=True); p.add_argument('--fixtures',required=True)
a=p.parse_args()
assert not os.environ.get('LIX_PROFILE_PERSISTED_REPLICAS'), 'paired timings require fresh replicas'
assert not os.environ.get('LIX_PROFILE_PREPARE_REPLICA'), 'preparation is a separate compatibility control'
records=[]
if a.summarize_only:
    records=[json.loads(line) for line in Path(a.output).read_text().splitlines()]
    assert not any(r.get('failed') for r in records), 'cannot summarize a failed run'
else:
    with open(a.output,'w') as out:
        for pair in range(a.pairs):
            for label in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
                env=dict(os.environ,LIX_PROFILE_FIXTURES=a.fixtures,LIX_PROFILE_SAMPLES='1',LIX_PROFILE_CASES=a.cases,LIX_PROFILE_RTT_MS=str(a.rtt),LIX_PROFILE_REVISION=label)
                run=subprocess.run([getattr(a,label)],env=env,capture_output=True,text=True)
                for line in run.stdout.splitlines():
                    row=json.loads(line); row.update(pair=pair,variant=label)
                    row['history_native_bytes']=sum(v['bytes'] for k,v in row['history'].items() if k.startswith('native-'))
                    row['history_total_requests']=sum(v['attempts'] for v in row['history'].values())
                    row['history_total_bytes']=sum(v['bytes'] for v in row['history'].values())
                    row['history_descriptor_requests']=sum(v['attempts'] for k,v in row['history'].items() if k in ('descriptor','descriptor-watch'))
                    assert 'file_lookup' in row and 'file_lookup_warm_us' in row
                    row['file_native_requests']=sum(v['attempts'] for k,v in row['file_open'].items() if k.startswith('native-'))
                    row['file_native_bytes']=sum(v['bytes'] for k,v in row['file_open'].items() if k.startswith('native-'))
                    records.append(row); out.write(json.dumps(row)+'\n'); out.flush()
                if run.returncode:
                    out.write(json.dumps({'pair':pair,'variant':label,'failed':True,'returncode':run.returncode,'stderr':run.stderr})+'\n'); out.flush()
                    raise SystemExit(f'{label} pair {pair} failed; evidence retained in {a.output}')
                print(f'{pair+1}/{a.pairs} {label}',flush=True)
rng=random.Random(20260916)
summary={}
for case in a.cases.split(','):
    rows={(r['pair'],r['variant']):r for r in records if r['case']==case}
    pairs=[(rows[i,'baseline'],rows[i,'candidate']) for i in range(a.pairs)]
    for b,c in pairs:
        assert (b['snapshot_digest'],b['rows'],b['rtt_ms'],b.get('history_limit'))==(c['snapshot_digest'],c['rows'],c['rtt_ms'],c.get('history_limit'))
        assert (b['file_lookup'],b['file_lookup_rows'])==(c['file_lookup'],c['file_lookup_rows'])
    metrics={}
    for metric in ['open_us','file_open_us','file_lookup_warm_us','file_native_requests','file_native_bytes','history_us','warm_us','history_native_requests','history_native_bytes','history_total_requests','history_total_bytes','history_descriptor_requests']:
        if any(b[metric] == 0 for b,c in pairs):
            metrics[metric]={'baseline_median':statistics.median(b[metric] for b,c in pairs),'candidate_median':statistics.median(c[metric] for b,c in pairs),'paired_improvement_median':None,'bootstrap_95_interval':None,'latency_threshold_pass':False,'baseline_range':[min(b[metric] for b,c in pairs),max(b[metric] for b,c in pairs)],'candidate_range':[min(c[metric] for b,c in pairs),max(c[metric] for b,c in pairs)],'note':'relative change undefined when a baseline count is zero; compare absolute counts'}
            continue
        improvements=[1-c[metric]/b[metric] for b,c in pairs]
        boots=sorted(statistics.median(rng.choices(improvements,k=len(improvements))) for _ in range(10000))
        metrics[metric]={'baseline_median':statistics.median(b[metric] for b,c in pairs),'candidate_median':statistics.median(c[metric] for b,c in pairs),'paired_improvement_median':statistics.median(improvements),'bootstrap_95_interval':[boots[249],boots[9749]],'latency_threshold_pass':boots[249]>.1,'baseline_range':[min(b[metric] for b,c in pairs),max(b[metric] for b,c in pairs)],'candidate_range':[min(c[metric] for b,c in pairs),max(c[metric] for b,c in pairs)]}
    summary[case]=metrics
Path(a.output+'.summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
