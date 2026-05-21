---
description: Versions are isolated lines of state. Create them, switch into them, read across them with _by_version tables, and merge with conflict-aware preview.
---

# Versions & Merging

A **version** in Lix is what Git calls a branch: an isolated line of state that can diverge from main and be merged back. Lix uses "version" because product UIs don't say "branch."

## Create and switch

```ts
const main = await lix.activeVersionId();

const draft = await lix.createVersion({ name: "Marketing edit" });
await lix.switchVersion({ versionId: draft.id });

// writes here are isolated to `draft`
await lix.execute(
  "UPDATE acme_section SET title = $1 WHERE id = $2",
  ["Sharper launch copy", "s1"],
);

await lix.switchVersion({ versionId: main });
```

`createVersion()` returns `{ id, name, hidden }`. `switchVersion()` is per-Lix-instance state; it changes which version subsequent SQL goes against.

Use names that match your callers' vocabulary. For an end-user product that's domain language: `"Marketing edit"`, `"Q3 pricing draft"`. For a CLI or infrastructure tool, developer terms like `"feature/x"` or `"staging"` are fine; Lix doesn't prescribe.

## Side-by-side reads with `_by_version`

Every registered schema `X` gets a sibling table `X_by_version` with a `lixcol_version_id` column. (Files and directories have the same shape: `lix_file_by_version`, `lix_directory_by_version`. For the full surface map see [SQL Surfaces](./surfaces.md).) Use it to read or write across versions without switching:

```ts
const sideBySide = await lix.execute(
  `SELECT v.name, s.title
     FROM acme_section_by_version s
     JOIN lix_version v ON v.id = s.lixcol_version_id
    WHERE s.id = $1
      AND s.lixcol_version_id IN ($2, $3)
    ORDER BY v.name`,
  ["s1", main, draft.id],
);
```

Rules for `_by_version`:

- `SELECT`: filter by `lixcol_version_id`, or omit the filter to scan all versions.
- `INSERT`: must include `lixcol_version_id`.
- `UPDATE` / `DELETE`: must include `lixcol_version_id` in the `WHERE` clause.
- The plain (non-suffixed) table is the active-version view.

Prefer `_by_version` for review UIs, sync, and any side-by-side rendering; it avoids the cost and risk of switching the active version.

## Preview a merge

`mergeVersionPreview()` reports the same merge decision as `mergeVersion()` without touching state.

```ts
const preview = await lix.mergeVersionPreview({ sourceVersionId: draft.id });

// preview shape:
// {
//   outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted",
//   targetVersionId, sourceVersionId,
//   baseCommitId, targetHeadCommitId, sourceHeadCommitId,
//   changeStats: { total, added, modified, removed },
//   conflicts: MergeConflict[],
// }
```

Outcomes:

- `alreadyUpToDate`: source has no commits the target lacks.
- `fastForward`: target advances to source without a merge commit.
- `mergeCommitted`: a new merge commit will be created.

`mergeVersion()` always merges into the **active** version. If you want a different target, switch to it first.

## Conflicts

If both versions modified the same entity since their merge base, `mergeVersionPreview()` returns them in `conflicts`, and `mergeVersion()` throws a `LixError`.

Each conflict has the shape:

```ts
{
  kind: "sameEntityChanged",
  schemaKey: "acme_section",
  entityPk: ["s1"],
  fileId: null,
  target: { kind: "added" | "modified" | "removed", beforeChangeId, afterChangeId },
  source: { kind: "added" | "modified" | "removed", beforeChangeId, afterChangeId },
}
```

Conflict detection is row-level today, not field-level: two versions editing different fields of the same row still conflict. Conflict semantics and resolution are an active roadmap item (see [Roadmap](https://github.com/opral/lix#roadmap)). **Don't reshape your schemas to avoid this**; design entities around how your code reads them, not around today's merge granularity.

Always wrap `mergeVersion()` when conflicts are possible:

```ts
try {
  const result = await lix.mergeVersion({ sourceVersionId: draft.id });
  console.log(result.outcome, result.changeStats.total);
} catch (error) {
  // resolve conflicts in calling code, then retry
}
```

## Don't shape entities around merge

It's tempting to split rows finely to dodge the row-level conflict rule. **Don't.** Schema design should follow how your code reads, writes, and joins data, not how today's merge engine resolves conflicts. Conflict semantics will improve; data models that work today should still work then.

If a domain naturally splits (a document into blocks, an invoice into line items, a translation set into per-key messages), split it because the *reads* want it that way. If the natural shape is one row with several fields, write it that way and handle conflicts in calling code when they happen. See [Schemas](./schemas.md#design-for-querying-not-for-merging).

## Hiding and deleting versions

`lix_version` is a writable system table. Hide a version from the active set without deleting it:

```ts
await lix.execute("UPDATE lix_version SET hidden = true WHERE id = $1", [draft.id]);
```

Delete a version with SQL:

```ts
await lix.execute("DELETE FROM lix_version WHERE id = $1", [draft.id]);
```

The engine refuses to delete the global version or the active version.
