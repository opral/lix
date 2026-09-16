import assert from 'node:assert/strict';
import test from 'node:test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { codeTree, selectContentArtifact, promoteContentArtifact } from './ci-content-artifact.mjs';
import { describeBrowser } from './release-browser-artifact.mjs';

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'content-artifact-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const git = (...args) => execFileSync('git', args, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  const write = (path, value) => { mkdirSync(dirname(join(root, path)), { recursive: true }); writeFileSync(join(root, path), value); };
  git('init'); git('config', 'user.name', 'CI'); git('config', 'user.email', 'ci@example.invalid');
  write('packages/js-sdk/src/index.ts', 'source'); git('add', '.'); git('commit', '-qm', 'code');
  const source = git('rev-parse', 'HEAD');
  for (const file of ['wasm/lix_js_sdk.js', 'wasm/lix_js_sdk.d.ts', 'wasm/lix_js_sdk_bg.wasm', 'migration-wasm/lix_js_sdk.js', 'migration-wasm/lix_js_sdk.d.ts', 'migration-wasm/lix_js_sdk_bg.wasm', 'bundled-plugins/plugin_csv.lixplugin', 'bundled-plugins/plugin_markdown.lixplugin']) write(`packages/js-sdk/dist/${file}`, file);
  const manifest = describeBrowser(root, source, {});
  write('ci-artifact/browser.json', JSON.stringify(manifest));
  write('blog/post.md', 'post'); git('add', 'blog'); git('commit', '-qm', 'docs');
  return {root, git, write, source, target: git('rev-parse', 'HEAD'), manifest};
}

test('whole-SDK reuse requires identical TypeScript as well as compiler inputs', t => {
  const f = fixture(t);
  assert.equal(codeTree(f.root, f.source), codeTree(f.root, f.target));
  promoteContentArtifact(f.root, f.source, f.target, '42', {});
  const manifest = JSON.parse(readFileSync(join(f.root, 'ci-artifact/browser.json')));
  assert.equal(manifest.sourceRevision, f.target);
  assert.equal(manifest.reusedFromRevision, f.source);
  assert.equal(manifest.releaseBuild.sourceTree, f.git('rev-parse', 'HEAD^{tree}'));
  f.write('packages/js-sdk/src/index.ts', 'changed code'); f.git('add', 'packages/js-sdk/src'); f.git('commit', '-qm', 'code');
  assert.throws(() => promoteContentArtifact(f.root, f.source, f.git('rev-parse', 'HEAD'), '42', {}), /code inputs differ/);
});

test('corrupt or incompatible browser outputs cannot be relabeled', t => {
  const f = fixture(t);
  assert.throws(() => promoteContentArtifact(f.root, f.source, f.target, '42', {LIX_WASM_PROFILE:'dev'}), /provenance/);
  f.write('packages/js-sdk/dist/wasm/lix_js_sdk_bg.wasm', 'corrupt');
  assert.throws(() => promoteContentArtifact(f.root, f.source, f.target, '42', {}), /checksum/);
});

test('lookup requires successful same-repository CI and retained matching provenance', async t => {
  const f = fixture(t);
  const run = {id:42, conclusion:'success', path:'.github/workflows/ci.yml', head_sha:f.source, head_repository:{full_name:'opral/lix'}};
  const artifacts = [{name:`lix-browser-sdk-${f.source}`}, {name:'ci-browser-build'}];
  const github = {rest:{actions:{listWorkflowRuns:async()=>({data:{workflow_runs:[run]}}), listWorkflowRunArtifacts(){}}}, paginate:async()=>artifacts};
  const select = async () => {
    const outputs = {};
    await selectContentArtifact({github, context:{repo:{owner:'opral',repo:'lix'}}, root:f.root, env:{}, readJson:async()=>f.manifest, core:{setOutput:(k,v)=>outputs[k]=v,info(){},warning(){}}});
    return outputs;
  };
  assert.deepEqual(await select(), {revision:f.source,run_id:42});
  for (const [obj,key,value] of [[run,'conclusion','failure'],[run.head_repository,'full_name','fork/lix'],[artifacts[0],'expired',true],[f.manifest.releaseBuild.binaries,'key','wrong-settings']]) {
    const old=obj[key];obj[key]=value;assert.deepEqual(await select(),{});obj[key]=old;
  }
  github.paginate=async()=>{throw new Error('unavailable')};
  assert.deepEqual(await select(),{});
});
