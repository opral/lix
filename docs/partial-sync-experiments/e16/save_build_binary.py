import argparse,json,shutil,hashlib
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('messages');p.add_argument('worktree');p.add_argument('output');a=p.parse_args()
rows=[]
for line in Path(a.messages).read_text().splitlines():
 try: rows.append(json.loads(line))
 except json.JSONDecodeError: pass
matches=[r for r in rows if r.get('reason')=='compiler-artifact' and r.get('target',{}).get('name')=='partial_replica_history' and r.get('executable')]
assert len(matches)==1, matches
r=matches[0];assert Path(r['target']['src_path']).resolve()==Path(a.worktree,'packages/e2e/benches/partial_replica_history.rs').resolve()
assert rows[-1].get('reason')=='build-finished' and rows[-1]['success']
shutil.copy2(r['executable'],a.output)
h=hashlib.sha256()
with open(a.output,'rb') as f:
 while chunk:=f.read(1024*1024):h.update(chunk)
evidence={'artifact':r,'saved_binary_sha256':h.hexdigest(),'harness_sha256':hashlib.sha256(Path(r['target']['src_path']).read_bytes()).hexdigest()}
Path(a.output+'.artifact.json').write_text(json.dumps(evidence,indent=2)+'\n')
print(json.dumps({'source':r['target']['src_path'],'executable':r['executable'],'saved':a.output,'sha256':h.hexdigest()}))
