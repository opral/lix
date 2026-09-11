#!/usr/bin/env python3
"""Run only after parent validates native tests and installs optimized WASM.
No builds. Native test binary is supplied explicitly, never guessed.
"""
import argparse, hashlib, json, os, pathlib, subprocess, time, statistics, math
P=argparse.ArgumentParser();P.add_argument('--native-binary',required=True);P.add_argument('--output',required=True);P.add_argument('--repo',default=str(pathlib.Path(__file__).resolve().parents[3]));a=P.parse_args()
repo=pathlib.Path(a.repo);out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=True)
wasm=repo/'packages/js-sdk/dist/wasm/lix_js_sdk_bg.wasm'
def digest(p):
 h=hashlib.sha256()
 with open(p,'rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def source_manifest():
 raw=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=repo)
 excluded_dirs={'node_modules','target','dist','build','coverage','test-results','playwright-report','.cache','artifacts','logs'}
 source_suffixes={'.rs','.ts','.tsx','.js','.jsx','.mjs','.cjs','.json','.toml','.yaml','.yml','.lock','.proto','.sql','.wit','.snap','.md','.sh','.py','.html','.css'}
 entries=[]
 for name in sorted(set(os.fsdecode(item) for item in raw.split(b'\0') if item)):
  path=pathlib.PurePosixPath(name)
  if any(part in excluded_dirs for part in path.parts):continue
  if name.startswith('packages/js-sdk/src/wasm/'):continue # generated wasm-bindgen files
  if path.suffix not in source_suffixes and path.name not in {'Cargo.lock','Dockerfile','Makefile','.gitignore','.npmrc'}:continue
  local=repo/name
  if not local.exists():
   entries.append({'path':name,'missing':True});continue
  if local.is_symlink():
   value=os.fsencode(os.readlink(local));entries.append({'path':name,'symlinkSha256':hashlib.sha256(value).hexdigest()})
  elif local.is_file():entries.append({'path':name,'sha256':digest(local)})
 encoded=json.dumps(entries,ensure_ascii=True,separators=(',',':'),sort_keys=True).encode()
 return {'sha256':hashlib.sha256(encoded).hexdigest(),'files':entries,
  'policy':'git ls-files --cached --others --exclude-standard, sorted unique paths; source/config/lock/snapshot/document extensions; excludes generated/build/dependency/artifact/log directories and SDK generated wasm-bindgen source; missing tracked files and symlink targets recorded.'}
def sdk_artifact_manifest():
 # Worker modules, generated binding glue and bundled plugins affect browser
 # behavior just as the WASM binary does. Pin the complete installed SDK.
 entries=[]
 for root in [repo/'packages/js-sdk/dist',repo/'packages/js-sdk/src/wasm']:
  if not root.exists():continue
  for path in sorted(root.rglob('*')):
   if path.is_file():entries.append({'path':str(path.relative_to(repo)),'sha256':digest(path)})
 encoded=json.dumps(entries,separators=(',',':'),sort_keys=True).encode()
 return {'sha256':hashlib.sha256(encoded).hexdigest(),'files':entries}
initial_sdk=sdk_artifact_manifest()
(out/'sdk-artifact-manifest.json').write_text(json.dumps(initial_sdk,indent=2)+'\n')
def assert_artifacts_unchanged():
 assert sdk_artifact_manifest()['sha256']==initial_sdk['sha256'], 'Installed SDK artifacts changed during profiling'
 assert digest(a.native_binary)==provenance['nativeBinarySha256'], 'Native authority artifact changed during profiling'
initial_sources=source_manifest()
(out/'source-manifest-start.json').write_text(json.dumps(initial_sources,indent=2)+'\n')
provenance={'wasmSha256':digest(wasm),'nativeBinarySha256':digest(a.native_binary),'gitHead':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'dirtyDiffSha256':hashlib.sha256(subprocess.check_output(['git','diff','HEAD'],cwd=repo)).hexdigest(),'sourceManifestSha256':initial_sources['sha256'],'sdkArtifactManifestSha256':initial_sdk['sha256'],'runs':[]}
def run(label,fixture,config,large=False,dimension=None):
 assert_artifacts_unchanged()
 folder=out/label;folder.mkdir(exist_ok=False)
 manifest=folder/'authority.json';stop=folder/'stop';result=folder/'result.json'
 env=os.environ|{'LIX_PARTIAL_PROFILE_MANIFEST':str(manifest),'LIX_PARTIAL_PROFILE_STOP':str(stop)}
 if large:env['LIX_PARTIAL_PROFILE_LARGE_BLOBS']='1'
 else:env.pop('LIX_PARTIAL_PROFILE_LARGE_BLOBS',None)
 native_log=open(folder/'authority.log','w');server=subprocess.Popen([a.native_binary,'--ignored','--exact',fixture,'--nocapture','--test-threads=1'],cwd=repo,env=env,stdout=native_log,stderr=subprocess.STDOUT)
 try:
  deadline=time.monotonic()+1200
  while not manifest.exists():
   if server.poll() is not None:raise RuntimeError(f'{label}: authority exited; inspect log')
   if time.monotonic()>deadline:raise TimeoutError(f'{label}: authority seeding exceeded20minutes')
   time.sleep(.25)
  entries=json.loads(manifest.read_text());selected=entries
  if large:
   selected=[x for x in entries if x['dimension']=='blob_320mib']
   assert len(selected)==1 and selected[0]['physicalBlobBytes']>=300*1024*1024
   assert selected[0]['logicalBlobBytes']==320*1024*1024
  elif dimension is not None:
   selected=[x for x in entries if x['dimension']==dimension]
   assert len(selected)==1, f'Expected one {dimension} fixture'
   if dimension=='rows_16':
    assert selected[0]['size']==16 and selected[0].get('logicalBlobBytes',0)==0
  browser_manifest=folder/'browser-manifest.json';browser_manifest.write_text(json.dumps(selected,indent=2))
  env.update(LIX_PARTIAL_PROFILE_MANIFEST=str(browser_manifest),LIX_PARTIAL_PROFILE_RESULT=str(result))
  # Each invocation creates a fresh browser process and unique OPFS databases.
  # Each run gets a fresh authority; the file workflow includes a remote insertion.
  with open(folder/'browser.log','w') as log:
   subprocess.run(['./node_modules/.bin/vitest','run','--config',config,'--reporter=verbose'],cwd=repo/'packages/storage-opfs',env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=1200)
  assert_artifacts_unchanged()
  payload=json.loads(result.read_text())
  for row in payload.get('results',[]):
   if 'coldOpenMs' in row and row.get('beforeFirstAuthorityRequestMs') is not None:
    row['afterFirstAuthorityRequestToOpenMs']=row['coldOpenMs']-row['beforeFirstAuthorityRequestMs']
    row['openingTimingNote']='Total includes worker/WASM/OPFS startup. Residual starts at first authority request; it is not pure engine or network time.'
  summaries=[]
  for row in payload.get('results',[]):
   summary={k:row[k] for k in ['dimension','size','mode','rows','coldOpenMs','beforeFirstAuthorityRequestMs','afterFirstAuthorityRequestToOpenMs','foregroundOpeningRequests','foregroundOpeningResponseBytes','foregroundOpeningBytes','offlineInputReadAttempts'] if k in row}
   for key in ['warmSelectMs','warmUpdateMs','warmCountMs','selectMs','updateMs']:
    values=row.get(key,[])
    if values:
     ordered=sorted(values);summary[key]={'samples':len(values),'median':statistics.median(values),'p95':ordered[math.ceil(len(values)*.95)-1]}
   if 'timings' in row:
    summary['timings']=row['timings']
    summary['retainedFileReadSemantics']='Ordinary SELECT after background publication; this phase is already warm. First-SELECT-only offline editing is covered separately. Historical preparation phases used different APIs and are not directly comparable.'
    summary['remotePublicationSemantics']='Elapsed from remote INSERT acknowledgment through detection, including remote session close and polling overhead.'
   summaries.append(summary)
  (folder/'summary.json').write_text(json.dumps(summaries,indent=2)+'\n')
  enriched=folder/'result-with-opening-phases.json';enriched.write_text(json.dumps(payload,indent=2)+'\n')
  provenance['runs'].append({'label':label,'manifest':selected,'result':str(result),'enrichedResult':str(enriched)})
 finally:
  # Browser completes/closes before stopping authority; await release of all
  # concurrent longpoll handlers before seeding the next repository.
  stop.touch()
  try:server.wait(timeout=45)
  except subprocess.TimeoutExpired:
   server.terminate()
   try:server.wait(timeout=10)
   except subprocess.TimeoutExpired:server.kill();server.wait()
  native_log.close()
  (out/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
base='handle::partial::browser_profile_authority::partial_browser_profile_authority'
files='handle::partial::browser_file_profile_authority::partial_browser_file_profile_authority'
run('file-batching',files,'vitest.partial-file-sync.config.ts')
run('paired-local-sql',base,'vitest.partial-local-comparison.config.ts')
# Alternate order to reduce monotonic host warmup/order bias. Every arm seeds
# a new authority process and launches a new Chromium process; no repository or
# OPFS database is reused between arms or samples.
for sample in range(1,4):
 arms=['small','large'] if sample%2 else ['large','small']
 for arm in arms:
  run(f'{arm}-physical-{sample}',base,'vitest.partial-sync.config.ts',
      large=arm=='large',dimension='rows_16' if arm=='small' else None)
comparison=[]
for arm in ['small','large']:
 rows=[]
 for sample in range(1,4):
  item=json.loads((out/f'{arm}-physical-{sample}'/'summary.json').read_text())
  assert len(item)==1
  rows.append(item[0])
 metrics={}
 for key in ['coldOpenMs','beforeFirstAuthorityRequestMs','afterFirstAuthorityRequestToOpenMs','foregroundOpeningResponseBytes','foregroundOpeningRequests']:
  values=[row[key] for row in rows if row.get(key) is not None]
  metrics[key]={'samples':values,'median':statistics.median(values)} if values else None
 comparison.append({'arm':arm,'metrics':metrics})
(out/'opening-comparison.json').write_text(json.dumps({
 'artifactSha256':provenance['wasmSha256'],'results':comparison,
 'limits':'Three independent fresh-browser/authority samples per arm. Total includes startup; request-to-open residual is not pure network/engine time. No p95 population claim from three samples. Large physical CAS sizes are verified by native fixture and retained in provenance manifests.'},indent=2)+'\n')

assert_artifacts_unchanged()
provenance['sdkAndNativeArtifactsVerifiedUnchanged']=True
final_sources=source_manifest()
(out/'source-manifest-end.json').write_text(json.dumps(final_sources,indent=2)+'\n')
assert final_sources['sha256']==initial_sources['sha256'], 'Source tree changed during profiling; inspect source manifests'
provenance['sourceManifestVerifiedUnchanged']=True
(out/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
