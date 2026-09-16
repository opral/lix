import assert from 'node:assert/strict';
import test from 'node:test';
import { sdkBuildPlan, executeBuildPlan } from './ci-build-sdk.mjs';

for (const runtime of ['native', 'browser']) {
  test(`${runtime} builds isolate artifacts, preserve profiles, and share one runner budget`, () => {
    const plan = sdkBuildPlan(runtime, '/repo', {RUNNER_TEMP:'/temp', LIX_NATIVE_PROFILE:'test'}, 30);
    assert.equal(plan.length, runtime === 'native' ? 5 : 3);
    assert.equal(new Set(plan.map(p => p.env.CARGO_TARGET_DIR)).size, plan.length);
    assert.ok(plan.reduce((sum,p)=>sum+Number(p.env.CARGO_BUILD_JOBS),0)<=30);
    for (const phase of plan) {
      assert.equal(phase.env.LIX_OFFLINE_MIGRATION, phase.name.startsWith('migration-')?'1':'0');
      if (phase.name.includes('wasm')) assert.equal(phase.env.LIX_WASM_PROFILE, runtime==='native'?'dev':'release');
      assert.equal(phase.env.LIX_NATIVE_PROFILE,'test');
    }
    assert.equal(plan.filter(p=>p.name==='plugins').length,1);
  });
}

test('phases overlap and failure waits for all builds instead of abandoning children', async () => {
  const plan = sdkBuildPlan('browser','/repo',{},30);
  const started=[];
  const finished=[];
  let release;
  const barrier=new Promise(resolve=>{release=resolve});
  const result=executeBuildPlan(plan,async phase=>{
    started.push(phase.name);
    if(started.length===plan.length) release();
    await barrier;
    finished.push(phase.name);
    if(phase.name==='wasm') throw new Error('compiler failed');
  });
  await assert.rejects(result,AggregateError);
  assert.equal(started.length,plan.length);
  assert.equal(finished.length,plan.length);
  assert.throws(()=>sdkBuildPlan('unknown','/repo'),/Invalid/);
});
