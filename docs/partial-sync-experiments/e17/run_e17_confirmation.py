from pathlib import Path
import os,time,subprocess,sys,hashlib,json
p=Path('/root/repos/history-sync-research')
while Path('/proc/592764').exists():time.sleep(5)
assert 'E17 perf matrix complete' in (p/'e17-perf-orchestrator.log').read_text()
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/lix-transparent-target',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0');bins={}
for label,tree in [('baseline','lix-partial-sync-tree-scope'),('candidate','lix-partial-sync-combined')]:
 w=Path('/root/repos')/tree;h=w/'packages/e2e/benches/partial_replica_history.rs';original=h.read_bytes()
 assert hashlib.sha256(original).hexdigest()=='6b3637e5f2262f22285f70710e1f1d63e9d2c5d1431d42621bf8a30fafd9249f'
 try:
  h.write_bytes((p/'e17-confirmation-harness.rs').read_bytes())
  with (p/f'e17-confirmation-{label}-build.jsonl').open('w') as out,(p/f'e17-confirmation-{label}-build.log').open('w') as err:
   subprocess.run(['cargo','build','--manifest-path','tooling/Cargo.toml','--config','profile.dev.package.lix.opt-level=2','-p','lix_e2e','--features','server-protocol','--bench','partial_replica_history','--message-format=json-render-diagnostics'],cwd=w,env=env,stdout=out,stderr=err,check=True)
  rows=[json.loads(l) for l in (p/f'e17-confirmation-{label}-build.jsonl').read_text().splitlines()]
  artifacts=[r for r in rows if r.get('reason')=='compiler-artifact' and r['target']['name']=='partial_replica_history' and r.get('executable')]
  assert len(artifacts)==1 and rows[-1].get('success');a=artifacts[0];assert Path(a['target']['src_path']).resolve()==h.resolve();bins[label]=a['executable']
  digest=hashlib.file_digest(open(a['executable'],'rb'),'sha256').hexdigest()
  (p/f'e17-confirmation-{label}.artifact.json').write_text(json.dumps({'artifact':a,'binary_sha256':digest,'harness_sha256':hashlib.sha256(h.read_bytes()).hexdigest()},indent=2)+'\n')
 finally:h.write_bytes(original)
 print(label+' uninstrumented confirmation built; harness restored',flush=True)
for warmup in [0,500]:
 output=p/f'e17-confirmation-warmup{warmup}.jsonl';assert not output.exists()
 env=dict(os.environ,LIX_PROFILE_FILE_LOOKUP='id',LIX_PROFILE_HISTORY_LIMIT='1',LIX_PROFILE_WARM_REPEATS='20',LIX_PROFILE_WARM_IDLE_MS='0',LIX_PROFILE_CPU_WARMUP_MS=str(warmup))
 with (p/f'e17-confirmation-warmup{warmup}-runner.log').open('w') as log:
  subprocess.run(['taskset','-c','2',sys.executable,str(p/'paired_file_selection.py'),bins['baseline'],bins['candidate'],'--pairs','10','--cases','wide16000','--rtt','25','--fixtures',str(p/'fixtures'),'--output',str(output)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 print(f'warmup{warmup} ten uninstrumented pairs complete',flush=True)
print('E17 uninstrumented confirmation complete',flush=True)
