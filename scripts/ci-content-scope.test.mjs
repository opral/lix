import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import test from 'node:test';
import { isContentPath, selectContentScope } from './ci-content-scope.mjs';

test('only independent content/site trees are exempt', () => {
  for (const path of ['blog/009/index.md', 'blog/009/cover.png', 'blog/table_of_contents.json', 'docs/server-protocol.md', 'website/package-lock.json']) assert.equal(isContentPath(path), true);
  for (const path of ['README.md', 'packages/lix/src/init_readme.md', 'packages/js-sdk/src/index.ts', '.github/workflows/ci.yml', 'Cargo.toml', 'new/input.md', 'blog/file\nCargo.toml']) assert.equal(isContentPath(path), false);
});

test('actual merge and complete push ranges; uncertain history and renamed code retain CI', t => {
  const cwd = mkdtempSync(join(tmpdir(), 'content-scope-'));
  t.after(() => rmSync(cwd, { recursive: true, force: true }));
  const git = (...args) => execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  const write = (path, value) => { mkdirSync(dirname(join(cwd, path)), { recursive: true }); writeFileSync(join(cwd, path), value); };
  const commit = () => { git('add', '.'); git('commit', '-qm', 'fixture'); return git('rev-parse', 'HEAD'); };
  git('init', '-b', 'main'); git('config', 'user.name', 'CI'); git('config', 'user.email', 'ci@example.invalid');
  write('engine.rs', 'initial'); const base = commit();
  git('checkout', '-qb', 'docs'); write('blog/post.md', 'post'); const head = commit();
  const event = { pull_request: { head: { sha: head } } };
  const prScope = () => selectContentScope({ eventName: 'pull_request', event, cwd });
  assert.equal(prScope(), false); // PR head alone does not prove the merge.
  git('checkout', 'main'); write('engine.rs', 'base advanced'); commit();
  git('merge', '--no-ff', 'docs', '-m', 'merge');
  assert.equal(prScope(), true);
  const after = git('rev-parse', 'HEAD');
  const push = before => selectContentScope({ eventName: 'push', event: { before, after }, cwd });
  assert.equal(push(base), false); // The earlier engine change is part of this push.
  assert.equal(push(git('rev-parse', 'HEAD^1')), true);
  assert.equal(push('0'.repeat(40)), false);
  assert.equal(selectContentScope({ eventName: 'workflow_dispatch', event, cwd }), false);
  assert.equal(selectContentScope({ eventName: 'push', event: { before: base, after, forced: true }, cwd }), false);
  const shallow = `${cwd}-shallow`;
  t.after(() => rmSync(shallow, { recursive: true, force: true }));
  git('clone', '--depth=1', `file://${cwd}`, shallow);
  assert.equal(selectContentScope({ eventName: 'pull_request', event, cwd: shallow }), false);
  git('checkout', '-qb', 'rename'); git('mv', 'engine.rs', 'blog/engine.md'); event.pull_request.head.sha = commit();
  git('checkout', 'main'); git('merge', '--no-ff', 'rename', '-m', 'rename');
  assert.equal(prScope(), false);
  assert.equal(selectContentScope({ eventName: 'pull_request', event: {}, cwd }), false);
});
