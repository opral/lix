from pathlib import Path
import os,time,subprocess,sys,hashlib
p=Path('/root/repos/history-sync-research');w=Path('/root/repos/lix-partial-sync-ancestor-pushdown')
while Path('/proc/346196').exists():time.sleep(5)
assert 'E16 exact controls and broad gates complete' in (p/'e16-exact-orchestrator.log').read_text()
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/lix-transparent-target',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0')
with (p/'e16-pushdown-focused.log').open('w') as log:
 subprocess.run(['cargo','nextest','run','-p','lix','--features','all-simulations,server-protocol','-E','test(scoped_path_index) | test(scoped_file_index_publication) | test(finite_file_id_selection) | test(indexed_file_content_capture)'],cwd=w,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
patch=subprocess.check_output(['git','diff','0b46fb063','--','packages/lix'],cwd=w);(p/'e16-pushdown-engine.patch').write_bytes(patch);print('Frozen pushdown '+hashlib.sha256(patch).hexdigest(),flush=True)
h=w/'packages/e2e/benches/partial_replica_history.rs';original=h.read_bytes();assert hashlib.sha256(original).hexdigest()=='6b3637e5f2262f22285f70710e1f1d63e9d2c5d1431d42621bf8a30fafd9249f'
try:
 h.write_bytes((p/'e16-deep-controls-harness.rs').read_bytes())
 with (p/'e16-pushdown-build.jsonl').open('w') as out,(p/'e16-pushdown-build.log').open('w') as err:
  subprocess.run(['cargo','build','--manifest-path','tooling/Cargo.toml','--config','profile.dev.package.lix.opt-level=2','-p','lix_e2e','--features','server-protocol','--bench','partial_replica_history','--message-format=json-render-diagnostics'],cwd=w,env=env,stdout=out,stderr=err,check=True)
 subprocess.run([sys.executable,str(p/'save_build_binary.py'),str(p/'e16-pushdown-build.jsonl'),str(w),str(p/'e16-pushdown-candidate-opt2')],check=True)
finally:h.write_bytes(original)
print('Pushdown binary built; canonical harness restored',flush=True)
env=dict(os.environ,LIX_PROFILE_FILE_LOOKUP='id',LIX_PROFILE_HISTORY_LIMIT='1')
for name,baseline,cases,rtt in [('vs-exact','e16-exact-candidate-opt2','deep4,deep16,deep64',0),('vs-accepted-rtt0','e16-deep-baseline-opt2','dense64,dirs1600,wide16000,deep4,deep16,deep64',0),('vs-accepted-rtt25','e16-deep-baseline-opt2','dense64,dirs1600,wide16000,deep4,deep16,deep64',25)]:
 output=p/f'e16-pushdown-{name}.jsonl';assert not output.exists()
 with (p/f'e16-pushdown-{name}-runner.log').open('w') as log:
  subprocess.run([sys.executable,str(p/'paired_file_selection.py'),str(p/baseline),str(p/'e16-pushdown-candidate-opt2'),'--pairs','10','--cases',cases,'--rtt',str(rtt),'--fixtures',str(p/'fixtures'),'--output',str(output)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 print(name+' pairs complete',flush=True)
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/lix-transparent-target',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0')
for name,cmd in [('nextest',['cargo','nextest','run','-p','lix','--features','all-simulations,server-protocol','--no-fail-fast']),('doctest',['cargo','test','-p','lix','--doc']),('fmt',['cargo','fmt','-p','lix','--','--check'])]:
 with (p/f'e16-pushdown-{name}.log').open('w') as log:result=subprocess.run(cmd,cwd=w,env=env,stdout=log,stderr=subprocess.STDOUT)
 print(name+': '+str(result.returncode),flush=True);assert result.returncode==0,name
print('E16 pushdown controls and broad gates complete',flush=True)
