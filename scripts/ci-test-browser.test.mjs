import assert from 'node:assert/strict';
import test from 'node:test';
import { runBrowserSuites } from './ci-test-browser.mjs';

test('package suites overlap while each retains built then packed-production checks', async () => {
  const calls=[];
  let release;
  const barrier=new Promise(resolve=>{release=resolve});
  await runBrowserSuites(async (directory,script)=>{
    calls.push([directory,script]);
    if(script==='test:browser:built') {
      if(calls.length===2) release();
      await barrier;
    }
  });
  for(const directory of ['packages/js-sdk','packages/storage-opfs']) assert.deepEqual(calls.filter(c=>c[0]===directory).map(c=>c[1]),['test:browser:built','test:browser:production']);
  assert.deepEqual(calls.slice(0,2).map(c=>c[1]),['test:browser:built','test:browser:built']);
});

test('a failed suite fails CI and still waits for its sibling', async () => {
  const calls=[];
  await assert.rejects(runBrowserSuites(async (directory,script)=>{
    if(directory==='packages/js-sdk') throw new Error('browser failed');
    await new Promise(resolve=>setTimeout(resolve,1));
    calls.push(script);
  }),AggregateError);
  assert.deepEqual(calls,['test:browser:built','test:browser:production']);
});
