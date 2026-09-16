from pathlib import Path
import time,subprocess,sys
p=Path('/root/repos/history-sync-research')
while Path('/proc/601900').exists():time.sleep(5)
assert 'E17 uninstrumented confirmation complete' in (p/'e17-confirmation-orchestrator.log').read_text()
# E16's queued extension was canceled before starting. Preserve that empty
# orchestrator record and run its still-relevant controls under E17 provenance.
assert not (p/'e16-combined-extra-rtt0.jsonl').exists()
subprocess.run([sys.executable,str(p/'run_e16_combined_extra.py')],check=True)
print('E17 remaining E16 controls complete',flush=True)
