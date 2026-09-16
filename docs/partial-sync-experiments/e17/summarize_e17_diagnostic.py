import json,statistics
from pathlib import Path
p=Path('/root/repos/history-sync-research');summary={}
for rtt,idle in [(25,0),(25,100),(0,0)]:
 f=p/f'e17-diagnostic-rtt{rtt}-idle{idle}.jsonl';rows=[json.loads(l) for l in f.read_text().splitlines()];assert len(rows)==10
 key=f'rtt{rtt}-idle{idle}';summary[key]={}
 for label in ['baseline','candidate']:
  rs=[r for r in rows if r['variant']==label];assert len(rs)==5 and all(r['trace_enabled'] for r in rs)
  samples=[w for r in rs for w in r['warm_samples']];assert len(samples)==100
  assert all(sum(v['attempts'] for k,v in w['network'].items() if k.startswith('native-'))==0 for w in samples)
  counts=sorted(set((w['counters']['decode']['count'],w['counters']['read']['count'],w['counters']['plan']['phases'][0]['calls'],w['counters']['plan']['phases'][0]['keys'],w['counters']['plan']['phases'][0]['bytes'],w['counters']['replay']['boundaries']) for w in samples))
  summary[key][label]={'replicas':5,'reads_per_replica':20,'first_median_us':statistics.median(r['warm_us'] for r in rs),'steady_median_of_replica_medians_us':statistics.median(statistics.median(w['us'] for w in r['warm_samples'][1:]) for r in rs),'steady_replica_medians_us':[statistics.median(w['us'] for w in r['warm_samples'][1:]) for r in rs], 'counter_tuples_decode_tree_reads_storage_calls_keys_bytes_replays':counts,'median_decode_ns':statistics.median(w['counters']['decode']['ns'] for w in samples),'median_tree_read_ns':statistics.median(w['counters']['read']['ns'] for w in samples),'median_hash_ns':statistics.median(w['counters']['hash']['ns'] for w in samples),'median_measured_io_ns':statistics.median(w['counters']['plan']['phases'][0]['io_ns'] for w in samples)}
(p/'e17-diagnostic-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
