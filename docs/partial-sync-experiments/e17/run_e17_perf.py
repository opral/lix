from pathlib import Path
import os,time,subprocess,sys,hashlib,json,tempfile,signal
p=Path('/root/repos/history-sync-research')
while Path('/proc/573489').exists():time.sleep(5)
assert 'E17 diagnostic matrix complete' in (p/'e17-diagnostic-orchestrator.log').read_text()
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/lix-transparent-target',CARGO_BUILD_JOBS='8',CARGO_INCREMENTAL='0');bins={}
for label,tree in [('baseline','lix-partial-sync-tree-scope'),('candidate','lix-partial-sync-combined')]:
 w=Path('/root/repos')/tree;h=w/'packages/e2e/benches/partial_replica_history.rs';original=h.read_bytes()
 assert hashlib.sha256(original).hexdigest()=='6b3637e5f2262f22285f70710e1f1d63e9d2c5d1431d42621bf8a30fafd9249f'
 try:
  h.write_bytes((p/'e17-perf-harness.rs').read_bytes())
  with (p/f'e17-perf-{label}-build.jsonl').open('w') as out,(p/f'e17-perf-{label}-build.log').open('w') as err:
   subprocess.run(['cargo','build','--manifest-path','tooling/Cargo.toml','--config','profile.dev.package.lix.opt-level=2','-p','lix_e2e','--features','server-protocol,root-replay-trace','--bench','partial_replica_history','--message-format=json-render-diagnostics'],cwd=w,env=env,stdout=out,stderr=err,check=True)
  rows=[json.loads(l) for l in (p/f'e17-perf-{label}-build.jsonl').read_text().splitlines()]
  artifacts=[r for r in rows if r.get('reason')=='compiler-artifact' and r['target']['name']=='partial_replica_history' and r.get('executable')]
  assert len(artifacts)==1 and rows[-1].get('success');a=artifacts[0]
  assert Path(a['target']['src_path']).resolve()==h.resolve();bins[label]=a['executable']
  digest=hashlib.file_digest(open(a['executable'],'rb'),'sha256').hexdigest()
  (p/f'e17-perf-{label}.artifact.json').write_text(json.dumps({'artifact':a,'binary_sha256':digest,'harness_sha256':hashlib.sha256(h.read_bytes()).hexdigest()},indent=2)+'\n')
 finally:h.write_bytes(original)
 print(label+' perf diagnostic built; harness restored',flush=True)
for name,affinity,warmup,rtt in [('natural',None,0,25),('pinned','2',0,25),('conditioned','2',500,25),('conditioned-rtt0','2',500,0)]:
 output=p/f'e17-perf-{name}.jsonl';assert not output.exists()
 with output.open('w') as out:
  for pair in range(5):
   for label in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
    stem=f'e17-perf-{name}-{pair}-{label}'
    with tempfile.TemporaryDirectory(prefix='e17-perf-') as temp:
     ctl=Path(temp)/'ctl';ack=Path(temp)/'ack';os.mkfifo(ctl);os.mkfifo(ack)
     env=dict(os.environ,LIX_PROFILE_FIXTURES=str(p/'fixtures'),LIX_PROFILE_SAMPLES='1',LIX_PROFILE_CASES='wide16000',LIX_PROFILE_FILE_LOOKUP='id',LIX_PROFILE_HISTORY_LIMIT='1',LIX_PROFILE_WARM_REPEATS='20',LIX_PROFILE_WARM_IDLE_MS='0',LIX_PROFILE_CPU_WARMUP_MS=str(warmup),LIX_PROFILE_RTT_MS=str(rtt),LIX_PROFILE_PERF_CTL=str(ctl),LIX_PROFILE_PERF_ACK=str(ack))
     cmd=['perf','stat','-D','-1','--control',f'fifo:{ctl},{ack}','-x',';','-o',str(p/(stem+'.csv')),'-e','cycles:u,instructions:u,task-clock','--']
     if affinity:cmd+=['taskset','-c',affinity]
     cmd+=[bins[label]]
     with (p/(stem+'.stdout.jsonl')).open('w') as stdout,(p/(stem+'.stderr.log')).open('w') as stderr:
      proc=subprocess.Popen(cmd,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
      try:code=proc.wait(timeout=180)
      except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait();raise
      assert code==0,(stem,code)
     rows=[json.loads(l) for l in (p/(stem+'.stdout.jsonl')).read_text().splitlines()];assert len(rows)==1
     row=rows[0];assert row['trace_enabled'];row.update(pair=pair,variant=label,affinity=affinity,perf_csv=stem+'.csv');out.write(json.dumps(row)+'\n');out.flush()
    print(name+f' pair{pair+1} '+label,flush=True)
print('E17 perf matrix complete',flush=True)
