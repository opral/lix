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

## Read branches side by side

Every registered schema `X` gets an `X_by_branch` table with a `lixcol_branch_id` column. Files and directories have the same pattern with `lix_file_by_branch` and `lix_directory_by_branch`.

```ts
const sideBySide = await lix.execute(
  `SELECT b.name, s.title
	 FROM acme_section_by_branch s
	 JOIN lix_branch b ON b.id = s.lixcol_branch_id
	 WHERE s.id = $1
	   AND s.lixcol_branch_id IN ($2, $3)
	 ORDER BY b.name`,
  ["s1", main, draft.id],
);
```

Rules for `_by_branch` tables:

- `SELECT` can read one or many branches.
- `INSERT` must include `lixcol_branch_id`.
- `UPDATE` and `DELETE` must filter by `lixcol_branch_id`.
- The plain table reads and writes the active branch.

Use `_by_branch` tables for review UIs and side-by-side views. See [SQL Surfaces](./surfaces.md) for the full table map.

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
//   conflicts: MergeConflict[],
//   ...
// }
```

`mergeBranch()` always merges into the active branch. Switch to the target branch before previewing or merging. Merging a branch into itself throws an error.

```ts
await lix.switchBranch({ branchId: main });

const preview = await lix.mergeBranchPreview({ sourceBranchId: draft.id });
if (preview.conflicts.length === 0) {
  await lix.mergeBranch({ sourceBranchId: draft.id });
}
```

## Conflicts

If both branches changed the same row after their merge base, the preview includes a `sameRowChanged` conflict. `mergeBranch()` throws a `LixError` until the caller resolves it.

```ts
{
	kind: "sameRowChanged",
	schemaKey: "acme_section",
	rowPk: ["s1"],
	fileId: null,
	target: { kind: "modified", beforeChangeId, afterChangeId },
	source: { kind: "modified", beforeChangeId, afterChangeId },
}
```

Conflict detection is row-level today. Two branches that edit different fields of the same row still conflict. Design rows for how your app reads them, not around the current merge rule.

## Hide or delete a branch

`lix_branch` is a writable system table:

```ts
await lix.execute("UPDATE lix_branch SET hidden = true WHERE id = $1", [
  draft.id,
]);
await lix.execute("DELETE FROM lix_branch WHERE id = $1", [draft.id]);
```

Lix creates a built-in branch named `global` when it opens a repository. You
cannot delete that branch, and you cannot delete the active branch.

`hidden` only marks a branch for UIs. It does not change what SQL queries can see.
