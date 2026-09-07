import assert from 'node:assert/strict';
import test from 'node:test';
import { findReusableRun, matchesTestedSource, selectMergeReuse } from './ci-merge-reuse.mjs';

const evidence = { schemaVersion: 1, sourceRevision: 'head', sourceTree: 'tree', testedTree: 'tree' };
test('reuse requires both the SDK head tree and Rust merge tree to match', () => {
  assert.equal(matchesTestedSource(evidence, { revision: 'head', tree: 'tree' }), true);
  for (const patch of [{ sourceTree: 'old' }, { testedTree: 'old' }, { sourceRevision: 'old' }, { schemaVersion: 2 }]) {
    assert.equal(matchesTestedSource({ ...evidence, ...patch }, { revision: 'head', tree: 'tree' }), false);
  }
});
function fixture() {
  const pr = { merged_at: 'date', merge_commit_sha: 'merge', head: { sha: 'head', repo: { full_name: 'opral/lix' } } };
  const run = { id: 42, conclusion: 'success', event: 'pull_request', head_sha: 'head', head_repository: { full_name: 'opral/lix' }, path: '.github/workflows/ci.yml' };
  const artifacts = [{ name: 'ci-tested-source' }, { name: 'lix-browser-sdk-head' }];
  const github = { rest: {
    repos: { listPullRequestsAssociatedWithCommit: async () => ({ data: [pr] }) },
    actions: { listWorkflowRuns: async () => ({ data: { workflow_runs: [run] } }), listWorkflowRunArtifacts() {} },
  }, paginate: async () => artifacts };
  return { pr, run, artifacts, args: { github, repository: 'opral/lix', sha: 'merge', tree: 'tree', readEvidence: async () => evidence } };
}
test('successful matching PR run retains a downstream SDK artifact without recompilation', async () => {
  assert.deepEqual(await findReusableRun(fixture().args), { runId: 42, revision: 'head' });
});
for (const [name, mutate] of [
  ['unmerged PR', f => f.pr.merged_at = null],
  ['different merge', f => f.pr.merge_commit_sha = 'other'],
  ['fork PR', f => f.pr.head.repo.full_name = 'fork/lix'],
  ['failed CI', f => f.run.conclusion = 'failure'],
  ['cancelled CI', f => f.run.conclusion = 'cancelled'],
  ['push CI', f => f.run.event = 'push'],
  ['different workflow', f => f.run.path = '.github/workflows/other.yml'],
  ['different head', f => f.run.head_sha = 'old'],
  ['fork run', f => f.run.head_repository.full_name = 'fork/lix'],
  ['expired SDK', f => f.artifacts[1].expired = true],
  ['missing provenance', f => f.artifacts.shift()],
  ['changed merge tree', f => f.args.tree = 'new-tree'],
]) {
  test(`${name} requires full CI`, async () => {
    const f = fixture(); mutate(f);
    assert.equal(await findReusableRun(f.args), null);
  });
}
test('manual dispatch and PR updates always validate', async () => {
  for (const eventName of ['workflow_dispatch', 'pull_request']) {
    const outputs = {};
    await selectMergeReuse({ github: {}, context: { eventName }, core: { setOutput: (k, v) => outputs[k] = v } });
    assert.deepEqual(outputs, { reuse: 'false' });
  }
});
test('API failure falls back to full CI', async () => {
  const outputs = {}; const warnings = [];
  await selectMergeReuse({ github: {}, context: { eventName: 'push', repo: { owner: 'opral', repo: 'lix' } }, core: {
    setOutput: (k, v) => outputs[k] = v, warning: message => warnings.push(message),
  } });
  assert.equal(outputs.reuse, 'false');
  assert.equal(warnings.length, 1);
});
