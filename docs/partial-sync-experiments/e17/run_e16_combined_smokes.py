from pathlib import Path
import os, subprocess
p=Path('/root/repos/history-sync-research')
assert 'E16 combined controls and broad gates complete' in (p/'e16-combined-orchestrator.log').read_text()
for mode in ['id','missing_id','id_batch16','id_batch256','path','missing_path','prefix','invalid_id']:
 output=p/f'e16-combined-{mode}-smoke.jsonl';assert not output.exists()
 env=dict(os.environ,LIX_PROFILE_FIXTURES=str(p/'fixtures'),LIX_PROFILE_SAMPLES='1',LIX_PROFILE_CASES='dense64,wide16000,dirs1600,deep64',LIX_PROFILE_FILE_LOOKUP=mode,LIX_PROFILE_HISTORY_LIMIT='1')
 with output.open('w') as out,(p/f'e16-combined-{mode}-smoke.log').open('w') as err:
  subprocess.run([str(p/'e16-combined-candidate-opt2')],env=env,stdout=out,stderr=err,check=True)
 print(mode+' smoke passed',flush=True)
