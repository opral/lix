import json,sys
from pathlib import Path
rows=0;max_bytes=0
for name in sys.argv[1:]:
 for line in Path(name).read_text().splitlines():
  r=json.loads(line);assert not r.get('failed'),name
  opening=r['opening'];counts={k:v['attempts'] for k,v in opening.items() if v['attempts']}
  assert counts=={'descriptor':1,'handshake':1},(name,counts)
  size=sum(v['bytes'] for v in opening.values());assert size<=8192,(name,size)
  assert not r['persistent_replica'] and not r['reopening'],name
  max_bytes=max(max_bytes,size);rows+=1
print(json.dumps({'rows_verified':rows,'opening_foreground_requests':2,'opening_native_requests':0,'max_opening_response_bytes':max_bytes}))
