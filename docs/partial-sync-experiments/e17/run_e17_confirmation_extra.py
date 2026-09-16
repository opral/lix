from pathlib import Path
import os,time,subprocess,sys,json
p=Path('/root/repos/history-sync-research')
# Wait for the already running query-form controls; do not overlap timing.
while True:
 processes=subprocess.check_output(['pgrep','-f','^python3 /root/repos/history-sync-research/run_e17_remaining_controls.py$'],text=True) if subprocess.run(['pgrep','-f','^python3 /root/repos/history-sync-research/run_e17_remaining_controls.py$'],stdout=subprocess.DEVNULL).returncode==0 else ''
 if not processes:break
 time.sleep(5)
assert 'E17 remaining E16 controls complete' in (p/'e17-remaining-controls-orchestrator.log').read_text()
bins={label:json.loads((p/f'e17-confirmation-{label}.artifact.json').read_text())['artifact']['executable'] for label in ['baseline','candidate']}
env=dict(os.environ,LIX_PROFILE_FILE_LOOKUP='id',LIX_PROFILE_HISTORY_LIMIT='1',LIX_PROFILE_WARM_REPEATS='20',LIX_PROFILE_WARM_IDLE_MS='0',LIX_PROFILE_CPU_WARMUP_MS='500')
base=['taskset','-c','2',sys.executable,str(p/'paired_file_selection.py'),bins['baseline'],bins['candidate'],'--cases','wide16000','--rtt','25','--fixtures',str(p/'fixtures')]
extra=p/'e17-confirmation-warmup500-extra.jsonl';assert not extra.exists()
with (p/'e17-confirmation-warmup500-extra-runner.log').open('w') as log:
 subprocess.run(base+['--pairs','10','--output',str(extra)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
rows=[json.loads(l) for l in (p/'e17-confirmation-warmup500.jsonl').read_text().splitlines()]
added=[json.loads(l) for l in extra.read_text().splitlines()]
for r in added:r['pair']+=10
merged=p/'e17-confirmation-warmup500-20pairs.jsonl';assert not merged.exists();merged.write_text(''.join(json.dumps(r)+'\n' for r in rows+added))
with (p/'e17-confirmation-warmup500-20pairs-summary.log').open('w') as log:
 subprocess.run(base+['--summarize-only','--pairs','20','--output',str(merged)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
print('E17 twenty-pair confirmation complete',flush=True)
