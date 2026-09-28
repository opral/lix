import assert from 'node:assert/strict';
import test from 'node:test';
import { runBrowserSuites } from './ci-test-browser.mjs';

test('package suites run sequentially with built then packed-production checks', async () => {
  const calls=[];
  await runBrowserSuites(async (directory,script)=>{
    calls.push([directory,script]);
  });
  assert.deepEqual(calls, [
    ['packages/js-sdk','test:browser:built'],
    ['packages/js-sdk','test:browser:production'],
    ['packages/storage-opfs','test:browser:built'],
    ['packages/storage-opfs','test:browser:production'],
  ]);
});

test('a failed suite stops the browser CI run', async () => {
  const calls=[];
  await assert.rejects(runBrowserSuites(async (directory,script)=>{
    calls.push([directory,script]);
    if(directory==='packages/js-sdk') throw new Error('browser failed');
  }),/browser failed/);
  assert.deepEqual(calls,[['packages/js-sdk','test:browser:built']]);
});
