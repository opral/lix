---
description: Give each agent an isolated branch, preview its changes, then merge or discard the result.
---

# Lix for AI Agents

Agents make fast, useful, and sometimes wrong changes. Lix gives each agent task its own branch so a human or policy can review the work before it reaches main.

Agents can work through normal files with `FilesystemStorage`, through SQL, or through a hosted Lix server. All writes stay isolated on the task branch.

## The pattern

1. Create a branch for the agent task.
2. Switch the agent to that branch.
3. Let the agent edit files or SQL rows.
4. Switch back to main and preview the merge.
5. Merge, iterate on the branch, or discard it.

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

Use `FilesystemStorage` when the agent works with files on disk. See
[Persistence and Storage](./persistence.md#local-filesystem) for setup.

## Hosted repository

Use remote mode when the repository runs on a server. See
[Persistence and Storage](./persistence.md#remote-server) for setup.

## Why branches matter

- Run agents in parallel without changing main.
- Compare proposed results side by side.
- Review the [diff](./diffs.md) instead of rereading every file.
- Discard a bad attempt without manual cleanup.

## Inspect the work

To review the agent's work before the merge, open another session on the task
branch and query the ordinary current-state relation:

```ts
const reviewLix = await lix.openAnotherSession({ branchId: task.id });
const rows = await reviewLix.execute(
  "SELECT id, title, status FROM acme_task ORDER BY id",
);
await reviewLix.close();
```

`lix_history('<schema>')` is revision history anchored to a commit; it is not a
current-state snapshot replacement. Use the branch-scoped session for current
rows and `lix_diff('acme_task', from_commit_id, to_commit_id)` for a
relation-specific commit-to-commit change set.

Use `lix_registered_schema` to discover available schemas. Use `lix_change` for activity across the whole repository. It is not limited to the active branch.

## Conflicts

Merge is per row today. Two branches that edit different rows can merge cleanly. Two branches that edit the same row produce a `sameRowChanged` conflict.

See [Branching](./branching.md) for preview results and conflict handling.

## Next

- [Getting Started](./getting-started.md): the basic setup.
- [Branching](./branching.md): previews, conflicts, and side-by-side reads.
- [History](./history.md): SQL for review and undo.
