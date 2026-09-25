---
description: Branches are independent lines of work. Create a branch, switch branches, compare their data, and preview a merge.
---

# Branching

A branch is an independent line of work. Changes on one branch do not affect other branches. You can merge the changes into another branch later.

## Create and switch

```ts
const main = await lix.activeBranchId();

const draft = await lix.createBranch({ name: "Marketing edit" });
await lix.switchBranch({ branchId: draft.id });

await lix.execute("UPDATE acme_section SET title = $1 WHERE id = $2", [
  "Sharper launch copy",
  "s1",
]);

await lix.switchBranch({ branchId: main });
```

`createBranch()` returns `{ id, name, hidden, commitId }`. `switchBranch()` sets the branch that later SQL statements read and write.

Use names that fit your product, such as `"Marketing edit"`, `"Q3 pricing draft"`, or `"Agent task 123"`.

## Work with branches concurrently

SQL relations always read and write the current session's active branch. Open another session to work with another branch without switching the primary one:

```ts
const draftLix = await lix.openAnotherSession({ branchId: draft.id });

const [mainRows, draftRows] = await Promise.all([
  lix.execute("SELECT id, title FROM acme_section ORDER BY id"),
  draftLix.execute("SELECT id, title FROM acme_section ORDER BY id"),
]);

await draftLix.close();
```

Each session has independent branch selection, transactions, observations, and lifecycle. To see what changed between the two branches, use `lix_diff('acme_section', mainCommit, draftCommit)` instead of comparing two result sets.

## Preview a merge

`mergeBranchPreview()` shows what `mergeBranch()` would do. It does not change any data.

```ts
const preview = await lix.mergeBranchPreview({
  sourceBranchId: draft.id,
});

// {
//   outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted",
//   targetBranchId,
//   sourceBranchId,
//   changeStats: { total, added, modified, removed },
//   ...
// }
```

`mergeBranch()` always merges into the active branch. Switch to the target branch before previewing or merging. Merging a branch into itself throws an error.

```ts
await lix.switchBranch({ branchId: main });

const preview = await lix.mergeBranchPreview({ sourceBranchId: draft.id });
await lix.mergeBranch({ sourceBranchId: draft.id });
```

## Automatic merging

Lix reconciles overlapping edits automatically. Changes to different columns of the same row combine. For competing changes to the same column, the incoming source value wins by default; a plugin can provide a column merger instead. Creation/deletion races use whole-row last-writer-wins (LWW).

“Last” follows acceptance order, not client timestamps. In a branch merge, the source branch is incoming. Overlapping edits do not require caller conflict resolution.

Undo/redo bookkeeping is an exception: independently changing the same receipt
or checkpoint-cycle state on both branches causes an atomic merge conflict,
even if the resulting content is equal. A fast-forward adopts the selected
source branch's working baseline with its head. See [Undo and redo](./undo-redo.md).

Preview reports the merge outcome and change counts; it does not reserve the branch heads or approve a later merge against changing data.

## Hide or delete a branch

`lix_branch` is a writable system table:

```ts
await lix.execute("UPDATE lix_branch SET hidden = true WHERE id = $1", [
  draft.id,
]);
await lix.execute("DELETE FROM lix_branch WHERE id = $1", [draft.id]);
```

A new repository opens on a branch named `main`. Lix also creates a hidden branch named `global` that holds repository-wide rows such as branch descriptors. You cannot delete `global`, and you cannot delete the active branch.

For every SQL table, `INSERT ... ON CONFLICT` targets the scope of the inserted
row. Omitting `lixcol_global` targets the active branch; setting it to `true`
targets the global branch. If a matching row exists in the inserted scope,
the upsert follows its `DO UPDATE` or `DO NOTHING` action. Otherwise, a row
with the same identity in the other visible scope causes a constraint error,
even with `DO NOTHING`. A plain `INSERT` can still create a local row that
shadows a global row. For example, to upsert a shared setting:

```sql
INSERT INTO lix_key_value (key, value, lixcol_global)
VALUES ('sync_enabled', 'true', true)
ON CONFLICT (key) DO UPDATE SET value = excluded.value;
```

`hidden` only marks a branch for UIs. It does not change what SQL queries can see.

## Branch metadata

Use `lix_branch.lixcol_metadata` to attach a JSON object to a branch:

```ts
await lix.execute("UPDATE lix_branch SET lixcol_metadata = $1 WHERE id = $2", [
  JSON.stringify({ owner: "design" }),
  draft.id,
]);
```

Metadata belongs to the tracked `lix_branch_descriptor` row, just as file and directory metadata belongs to their descriptors. Branch descriptors are global, so the metadata is visible from every branch. Renaming, hiding, or moving the branch head preserves it. Set `lixcol_metadata = NULL` to clear it.
