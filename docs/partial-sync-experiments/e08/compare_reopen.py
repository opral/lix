import argparse,json,os,shutil,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('baseline');p.add_argument('candidate');p.add_argument('prepared');p.add_argument('prefix');p.add_argument('--pairs',type=int,default=3);p.add_argument('--fixtures',required=True);a=p.parse_args()
rows=[]
with open(a.prefix+'.jsonl','w') as out:
 for pair in range(a.pairs):
  for label in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
   root=a.prefix+f'-stores-{pair}-{label}';shutil.copytree(a.prepared,root)
   env=dict(os.environ,LIX_PROFILE_FIXTURES=a.fixtures,LIX_PROFILE_SAMPLES='1',LIX_PROFILE_CASES='dense64,sparse64',LIX_PROFILE_PERSISTED_REPLICAS=root,LIX_PROFILE_REVISION=label)
   run=subprocess.run([getattr(a,label)],env=env,capture_output=True,text=True,timeout=90)
   for line in run.stdout.splitlines():
    r=json.loads(line);r.update(pair=pair,variant=label);assert r['reopening'];rows.append(r);out.write(json.dumps(r)+'\n');out.flush()
   if run.returncode:
    out.write(json.dumps({'pair':pair,'variant':label,'failed':True,'stderr':run.stderr})+'\n');out.flush();raise SystemExit('reopen control failed')
   print(pair,label,flush=True)
for pair in range(a.pairs):
 for case in ['dense64','sparse64']:
  b,c=[next(r for r in rows if r['pair']==pair and r['case']==case and r['variant']==v) for v in ['baseline','candidate']]
  assert (b['snapshot_digest'],b['rows'])==(c['snapshot_digest'],c['rows'])
print(json.dumps([{k:r[k] for k in ['case','pair','variant','history_native_requests','history','opening']} for r in rows],indent=2))
