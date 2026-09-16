from pathlib import Path
import os,sys,time,subprocess,json
p=Path('/root/repos/history-sync-research')
while Path('/proc/504003').exists():time.sleep(5)
assert 'E16 combined controls and broad gates complete' in (p/'e16-combined-orchestrator.log').read_text()
cases='dense64,deep16,deep64'
env=dict(os.environ,LIX_PROFILE_FILE_LOOKUP='id',LIX_PROFILE_HISTORY_LIMIT='1')
for rtt in [0,25]:
 extra=p/f'e16-combined-extra-rtt{rtt}.jsonl';assert not extra.exists()
 base=[sys.executable,str(p/'paired_file_selection.py'),str(p/'e16-deep-baseline-opt2'),str(p/'e16-combined-candidate-opt2'),'--cases',cases,'--rtt',str(rtt),'--fixtures',str(p/'fixtures')]
 with (p/f'e16-combined-extra-rtt{rtt}-runner.log').open('w') as log:
  subprocess.run(base+['--pairs','10','--output',str(extra)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 rows=[json.loads(line) for line in (p/f'e16-combined-vs-accepted-rtt{rtt}.jsonl').read_text().splitlines()]
 rows=[r for r in rows if r['case'] in cases.split(',')]
 added=[json.loads(line) for line in extra.read_text().splitlines()]
 for r in added:r['pair']+=10
 merged=p/f'e16-combined-20-paired-rtt{rtt}.jsonl';assert not merged.exists()
 merged.write_text(''.join(json.dumps(r)+'\n' for r in rows+added))
 with (p/f'e16-combined-20-paired-rtt{rtt}-summary.log').open('w') as log:
  subprocess.run(base+['--summarize-only','--pairs','20','--output',str(merged)],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 print(f'RTT{rtt}: twenty paired samples complete',flush=True)
subprocess.run([sys.executable,str(p/'run_e16_combined_smokes.py')],check=True)
print('E16 combined extended controls complete',flush=True)
