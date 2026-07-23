---
description: Route agent writes through Lix to get isolated workspaces, previewable changes, and approve-or-discard review for every agent task.
---

# Lix for AI Agents

Agent review is one of Lix's headline use cases, but the same primitives ([Versions](./versions.md), [Change History](./history.md)) power any product where end users review proposed changes. If you're building knowledge-work tools, the patterns here apply to humans drafting changes too.

Agents make fast, useful, and sometimes wrong changes. Lix gives each agent task its own isolated version of state so a human or a policy can review it before it lands.

## The pattern

1. Create a version for the agent task.
2. Switch the agent's writes into that version.
3. Run the agent. All writes are isolated.
4. Preview the merge: `changeStats` for the count, `conflicts` for collisions.
5. Approve, request changes, or discard.

```ts
const main = await lix.activeVersionId();

const task = await lix.createVersion({ name: "Agent task 123" });
await lix.switchVersion({ versionId: task.id });

// run the agent; every lix.execute is now isolated to `task`

await lix.switchVersion({ versionId: main });

const preview = await lix.mergeVersionPreview({ sourceVersionId: task.id });
if (preview.conflicts.length === 0) {
  await lix.mergeVersion({ sourceVersionId: task.id });
}
```

## Why versions matter for agents

- Run multiple agents in parallel without stepping on each other.
- Compare proposed outcomes side by side.
- Keep the main state stable while work is in progress.
- Discard a bad attempt with no manual cleanup.

## Showing the work

The point of routing agent writes through Lix is that you can ask SQL what the
agent did. Query the typed history relation for each application schema:

```sql
SELECT id, title, status, lixcol_depth,
       lixcol_observed_commit_id, lixcol_is_deleted
FROM acme_task_history
ORDER BY lixcol_depth, id;
```

Discover schemas through the read-only `lix_schema` catalog when the agent
works across applications. Use `lix_change` only for workspace-global activity; it is not
implicitly scoped to the agent's active branch. See
[Change History](./history.md) for the scope distinction and more recipes.

## Conflicts

Merge is per-entity today: two versions editing different rows merge cleanly; two versions editing the same row produce a `sameEntityChanged` conflict. Wrap `mergeVersion()` and handle the conflict in your review flow.

Don't reshape your schemas around this. Conflict semantics are still evolving; design entities for how your code reads them, not around today's merge granularity. See [Versions & Merging](./versions.md#dont-shape-entities-around-merge).

## Next

- [Getting Started](./getting-started.md): the basic loop.
- [Versions & Merging](./versions.md): preview shape, conflicts, side-by-side reads.
- [Change History](./history.md): the SQL surface for review and undo.
