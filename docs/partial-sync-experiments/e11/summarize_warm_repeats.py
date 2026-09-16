import json,pathlib,random,statistics,sys
path=pathlib.Path(sys.argv[1]); records=[json.loads(l) for l in path.read_text().splitlines()]
assert not any(r.get('failed') for r in records)
rows={(r['pair'],r['variant']):r for r in records}
pairs=sorted({r['pair'] for r in records})
assert all((i,v) in rows for i in pairs for v in ['baseline','candidate'])
rng=random.Random(20260916); out={}
for ordinal in range(5):
 b=[rows[i,'baseline']['warm_repeats_us'][ordinal] for i in pairs]
 c=[rows[i,'candidate']['warm_repeats_us'][ordinal] for i in pairs]
 gains=[1-y/x for x,y in zip(b,c)]
 boots=sorted(statistics.median(rng.choices(gains,k=len(gains))) for _ in range(10000))
 out[f'warm_{ordinal+1}']={'pairs':len(pairs),'baseline_median_us':statistics.median(b),'candidate_median_us':statistics.median(c),'paired_improvement':statistics.median(gains),'bootstrap_95_interval':[boots[249],boots[9749]],'baseline_values':b,'candidate_values':c}
pathlib.Path(str(path)+'.warm-summary.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out,indent=2))
