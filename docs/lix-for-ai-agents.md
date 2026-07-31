---
description: Give each agent an isolated branch, preview its changes, then merge or discard the result.
---

# Lix for AI Agents

Agents make fast, useful, and sometimes wrong changes. Lix gives each agent task its own branch so a human or policy can review the work before it reaches main.

Agents can work through normal files with `LocalFilesystem`, through SQL, or through a hosted Lix server. All writes stay isolated on the task branch.

## The pattern

1. Create a branch for the agent task.
2. Switch the agent to that branch.
3. Let the agent edit files or SQL rows.
4. Switch back to main and preview the merge.
5. Merge, request changes, or discard the branch.

```ts
const main = await lix.activeBranchId();

const task = await lix.createBranch({ name: "Agent task 123" });
await lix.switchBranch({ branchId: task.id });

// Run the agent. Its file and SQL writes are isolated to `task`.

await lix.switchBranch({ branchId: main });

const preview = await lix.mergeBranchPreview({ sourceBranchId: task.id });
if (preview.conflicts.length === 0) {
  await lix.mergeBranch({ sourceBranchId: task.id });
}
```

## Local file workspace

Use `LocalFilesystem` when the agent works with files on disk:

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new LocalFilesystem({ path: "./workspace", syncAllFiles: true }),
});
```

## Hosted workspace

Use remote mode when the workspace runs on a server:

```ts
const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/workspaces/acme",
  },
});
```

## Why branches matter

- Run agents in parallel without changing main.
- Compare proposed results side by side.
- Review semantic changes instead of rereading every file.
- Discard a bad attempt without manual cleanup.

## Inspect the work

Query typed history for each app or plugin schema:

```sql
SELECT id, title, status, lixcol_depth,
       lixcol_observed_commit_id, lixcol_is_deleted
FROM acme_task_history()
ORDER BY lixcol_depth, id;
```

Use `lix_registered_schema` to discover available schemas. Use `lix_change` for activity across the whole workspace. It is not limited to the active branch.

## Conflicts

Merge is per entity today. Two branches that edit different rows can merge cleanly. Two branches that edit the same row produce a `sameEntityChanged` conflict.

See [Branches & Merging](./versions.md) for preview results and conflict handling.

## Next

- [Getting Started](./getting-started.md): the basic setup.
- [Branches & Merging](./versions.md): previews, conflicts, and side-by-side reads.
- [Change History](./history.md): SQL for review and undo.
