---
description: Give each agent an isolated branch, preview its changes, then merge or discard the result.
---

# Lix for AI Agents

Give each agent task a branch for review before merging into main.
Agents can edit normal files or SQL rows, locally or through a hosted server.

## The pattern

1. Create and switch to a task branch.
2. Run the agent.
3. Return to main and preview the merge.
4. Merge, iterate, or discard.

```ts
const main = await lix.activeBranchId();

const task = await lix.createBranch({ name: "Agent task 123" });
await lix.switchBranch({ branchId: task.id });

// Run the agent. Its file and SQL writes are isolated to `task`.
// For example, the agent marks a task as done:
await lix.execute("UPDATE acme_task SET status = $1 WHERE id = $2", [
  "done",
  "T-1",
]);

await lix.switchBranch({ branchId: main });

const preview = await lix.mergeBranchPreview({ sourceBranchId: task.id });
// preview.changeStats is the one-line review summary:
// { total, added, modified, removed }
if (preview.conflicts.length === 0) {
  await lix.mergeBranch({ sourceBranchId: task.id });
}
```

## Local file repository

Use [`FilesystemStorage`](./persistence.md#local-filesystem) for files on disk.

## Hosted repository

For SDK-only access, [query the server directly](./persistence.md#remote-mode).
For files in a sandbox or mounted volume, [sync a filesystem replica](./persistence.md#filesystem-sync).
A browser can share the repository through an [OPFS replica](./persistence.md#browser-opfs).

## Why branches matter

- Run agents in parallel, isolated from main.
- Compare results and review [diffs](./diffs.md).
- Discard failed attempts.

## Inspect the work

Review current rows in a session on the task branch:

```ts
const reviewLix = await lix.openAnotherSession({ branchId: task.id });
const rows = await reviewLix.execute(
  "SELECT id, title, status FROM acme_task ORDER BY id",
);
await reviewLix.close();
```

Use `lix_history('<schema>')` for commit-anchored history, not current rows;
use `lix_diff('acme_task', from_commit_id, to_commit_id)` for changes between commits.

`lix_registered_schema` lists schemas. `lix_change` shows repository-wide
activity across branches.

## Conflicts

Merges operate per row: different-row edits can merge cleanly; same-row edits
produce `sameRowChanged`. See [Branching](./branching.md) for conflict handling.

## Next

- [Getting Started](./getting-started.md): the basic setup.
- [Branching](./branching.md): previews, conflicts, and side-by-side reads.
- [History](./history.md): SQL for review and undo.
