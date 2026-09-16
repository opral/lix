from pathlib import Path
import json,statistics,random
p=Path('/root/repos/history-sync-research');summary={}
for name in ['natural','pinned','conditioned','conditioned-rtt0']:
 rows=[json.loads(l) for l in (p/f'e17-perf-{name}.jsonl').read_text().splitlines()];assert len(rows)==10
 by={}
 for r in rows:
  e={}
  for l in (p/r['perf_csv']).read_text().splitlines():
   if l and not l.startswith('#'):
    c=l.split(';');assert float(c[4])==100.0,(name,c);e[c[2]]=float(c[0])
  r.update(cycles=e['cycles:u'],instructions=e['instructions:u'],cpu_ns=e['task-clock'],effective_ghz_equivalent=e['cycles:u']/e['task-clock'],steady_us=statistics.median(w['us'] for w in r['warm_samples'][1:]))
  by[r['pair'],r['variant']]=r
 summary[name]={};rng=random.Random(20260917)
 for metric in ['warm_us','steady_us','cycles','instructions','cpu_ns','effective_ghz_equivalent']:
  b=[by[i,'baseline'][metric] for i in range(5)];c=[by[i,'candidate'][metric] for i in range(5)];changes=[1-y/x for x,y in zip(b,c)];boot=sorted(statistics.median(rng.choices(changes,k=5)) for _ in range(10000))
  summary[name][metric]={'baseline_median':statistics.median(b),'candidate_median':statistics.median(c),'paired_improvement':statistics.median(changes),'paired_bootstrap95':[boot[249],boot[9749]],'baseline_values':b,'candidate_values':c}
(p/'e17-perf-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for name,v in summary.items():
 print(name,{m:(round(v[m]['baseline_median'],3),round(v[m]['candidate_median'],3),round(100*v[m]['paired_improvement'],2)) for m in ['steady_us','cycles','instructions','effective_ghz_equivalent']})
