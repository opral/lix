#!/usr/bin/env python3
"""Alternate two real benchmark executables on identical persisted fixtures."""
import argparse, json, os, random, statistics, subprocess
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('baseline'); p.add_argument('candidate'); p.add_argument('--pairs',type=int,default=10)
p.add_argument('--cases',default='dense64,sparse64'); p.add_argument('--rtt',type=int,default=0)
p.add_argument('--output',required=True); p.add_argument('--fixtures',required=True)
a=p.parse_args()
records=[]
with open(a.output,'w') as out:
    for pair in range(a.pairs):
        for label in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
            env=dict(os.environ,LIX_PROFILE_FIXTURES=a.fixtures,LIX_PROFILE_SAMPLES='1',LIX_PROFILE_CASES=a.cases,LIX_PROFILE_RTT_MS=str(a.rtt),LIX_PROFILE_REVISION=label)
            run=subprocess.run([getattr(a,label)],env=env,capture_output=True,text=True)
            for line in run.stdout.splitlines():
                row=json.loads(line); row.update(pair=pair,variant=label)
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
        assert (b['snapshot_digest'],b['rows'],b['rtt_ms'])==(c['snapshot_digest'],c['rows'],c['rtt_ms'])
    metrics={}
    for metric in ['open_us','file_open_us','history_us','warm_us','history_native_requests']:
        improvements=[1-c[metric]/b[metric] for b,c in pairs]
        boots=sorted(statistics.median(rng.choices(improvements,k=len(improvements))) for _ in range(10000))
        metrics[metric]={'baseline_median':statistics.median(b[metric] for b,c in pairs),'candidate_median':statistics.median(c[metric] for b,c in pairs),'paired_improvement_median':statistics.median(improvements),'bootstrap_95_interval':[boots[249],boots[9749]],'latency_threshold_pass':boots[249]>.1,'baseline_range':[min(b[metric] for b,c in pairs),max(b[metric] for b,c in pairs)],'candidate_range':[min(c[metric] for b,c in pairs),max(c[metric] for b,c in pairs)]}
    summary[case]=metrics
Path(a.output+'.summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
