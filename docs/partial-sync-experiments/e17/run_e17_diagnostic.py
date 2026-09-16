from pathlib import Path
import os,subprocess,sys,hashlib
p=Path('/root/repos/history-sync-research')
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/lix-transparent-target',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0')
for label,tree in [('baseline','lix-partial-sync-tree-scope'),('candidate','lix-partial-sync-combined')]:
 w=Path('/root/repos')/tree;h=w/'packages/e2e/benches/partial_replica_history.rs';original=h.read_bytes()
 assert hashlib.sha256(original).hexdigest()=='6b3637e5f2262f22285f70710e1f1d63e9d2c5d1431d42621bf8a30fafd9249f'
 try:
  h.write_bytes((p/'e17-warm-diagnostic-harness.rs').read_bytes())
  with (p/f'e17-{label}-build.jsonl').open('w') as out,(p/f'e17-{label}-build.log').open('w') as err:
   subprocess.run(['cargo','build','--manifest-path','tooling/Cargo.toml','--config','profile.dev.package.lix.opt-level=2','-p','lix_e2e','--features','server-protocol,root-replay-trace','--bench','partial_replica_history','--message-format=json-render-diagnostics'],cwd=w,env=env,stdout=out,stderr=err,check=True)
  subprocess.run([sys.executable,str(p/'save_build_binary.py'),str(p/f'e17-{label}-build.jsonl'),str(w),str(p/f'e17-{label}-diagnostic-opt2')],check=True)
 finally:h.write_bytes(original)
 print(label+' diagnostic built; harness restored',flush=True)
for rtt,idle in [(25,0),(25,100),(0,0)]:
 output=p/f'e17-diagnostic-rtt{rtt}-idle{idle}.jsonl';assert not output.exists()
 env=dict(os.environ,LIX_PROFILE_FILE_LOOKUP='id',LIX_PROFILE_HISTORY_LIMIT='1',LIX_PROFILE_WARM_REPEATS='20',LIX_PROFILE_WARM_IDLE_MS=str(idle))
 with (p/f'e17-diagnostic-rtt{rtt}-idle{idle}-runner.log').open('w') as log:
  subprocess.run([sys.executable,str(p/'paired_file_selection.py'),str(p/'e17-baseline-diagnostic-opt2'),str(p/'e17-candidate-diagnostic-opt2'),'--pairs','5','--cases','wide16000','--rtt',str(rtt),'--fixtures',str(p/'fixtures'),'--output',str(output)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 print(f'RTT{rtt} idle{idle} five diagnostic pairs complete',flush=True)
print('E17 diagnostic matrix complete',flush=True)
