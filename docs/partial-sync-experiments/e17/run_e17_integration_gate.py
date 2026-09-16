from pathlib import Path
import os,time,subprocess
p=Path('/root/repos/history-sync-research');w=Path('/root/repos/lix-partial-sync-warm-attribution')
while Path('/proc/621386').exists():time.sleep(5)
assert 'E17 twenty-pair confirmation complete' in (p/'e17-confirmation-extra-orchestrator.log').read_text()
# Integration includes release metadata 0.17.0 while controlled experiments
# used matching 0.16.1 trees. Validate the final integration environment too.
(w/'packages/lix/src/lib.rs').touch()
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/lix-transparent-target',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0')
for name,cmd in [('nextest',['cargo','nextest','run','-p','lix','--features','all-simulations,server-protocol','--no-fail-fast']),('doctest',['cargo','test','-p','lix','--doc']),('fmt',['cargo','fmt','-p','lix','--','--check'])]:
 with (p/f'e17-integration-{name}.log').open('w') as log:r=subprocess.run(cmd,cwd=w,env=env,stdout=log,stderr=subprocess.STDOUT)
 print(name+': '+str(r.returncode),flush=True);assert r.returncode==0,name
print('E17 integration engine gate complete',flush=True)
