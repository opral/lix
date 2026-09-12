---
description: Create restore points and query relation-specific changes between checkpoints and branch heads.
---

# Checkpoints

Lix automatically commits tracked changes. A checkpoint marks one of those
states as a user-meaningful restore point. Compare its commit with the active
branch head to inspect subsequent changes at the relation level your interface
uses.

```ts
const checkpoint = await lix.execute(
  "SELECT commit_id FROM lix_create_checkpoint()",
);
console.log("created checkpoint", checkpoint.rows[0].commit_id);
```

`lix_create_checkpoint()` checkpoints the active branch and returns the new
checkpoint commit ID. A full checkpoint is a metadata-only operation:

```sql
SELECT commit_id FROM lix_create_checkpoint();
```

## Complete example

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
await lix.execute("SELECT commit_id FROM lix_create_checkpoint()");

await lix.execute("INSERT INTO lix_key_value (key, value) VALUES ($1, $2)", [
  "checkpoint-demo",
  "draft",
]);

const working = await lix.execute(
  `SELECT row_ref, key, diff_type, from_value, to_value
   FROM lix_diff('lix_key_value')`,
);

for (const row of working.rows) {
  console.log(row.diff_type, row.key, row.row_ref);
}

await lix.execute("SELECT commit_id FROM lix_create_checkpoint()");
const remaining = await lix.execute(
  `SELECT count(*) AS count
   FROM lix_diff('lix_key_value')`,
);
console.assert(remaining.rows[0].count === 0);

await lix.close();
```

A runnable Rust version lives at
[`checkpoints.rs`](https://github.com/opral/lix/blob/main/packages/lix/examples/checkpoints.rs).

## Commit membership and queries

A checkpoint is a new immutable commit with `is_checkpoint = true`. Automatic
commits have the flag set to false. Empty checkpoints are retained log entries.
A selected checkpoint creates a marked commit and an ordinary child containing
remaining working changes. The flag cannot be updated through SQL.

```sql
SELECT commit_id, parent_commit_id, created_at
FROM lix_log()
WHERE is_checkpoint
ORDER BY position
LIMIT 20;

SELECT id, created_at
FROM lix_commit
WHERE is_checkpoint
ORDER BY created_at DESC, id DESC;
```

The first query is branch-relative; the second includes repository-global,
off-branch checkpoints. There is no separate `lix_checkpoint` relation or marker
write. Commit creation time is the single public checkpoint timestamp.

Use `lix_diff('lix_file')` for working changes. Its baseline is exposed as
`lix_branch.working_base_commit_id` and can be an ordinary commit after a fork or
restore. The latest marked commit is not necessarily the working baseline.

Full checkpoint publication aliases the captured state root; it does not scan
or copy the working interval. Physical reclamation runs in the background.
Selective checkpoint source dependencies remain available for offline sync.

See [History](./history.md) for log, endpoint history, snapshots, and paged
previews, and [Diff commands](./diff-commands.md) for scoped checkpoints.

Partial replicas upload ordinary pending edits and checkpoint dependencies in
bounded waves of at most 32 commits and 1 MiB of commit/ref JSON. Intermediate ordinary uploads keep
the authority's previous checkpoint; checkpoint publication waits for its native
dependencies. Lost replies resume the durable captured wave. Binary file content
uses the separate bounded blob uploader and is not counted as commit JSON.

A single commit or minimal checkpoint publication closure that exceeds the 1 MiB
request budget still requires multipart body preparation, which is not implemented
by this lane. Its local data remains pending; splitting a long sequence into waves
does not remove that atomic-body limit.
