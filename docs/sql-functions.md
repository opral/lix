---
description: Built-in scalar SQL functions and PostgreSQL JSONB syntax supported by Lix.
---

# SQL Functions

Lix exposes a small set of runtime functions. JSON uses PostgreSQL casts and
operators; there are no public `lix_json_*` functions.

| Function | Returns | Purpose |
| :-- | :-- | :-- |
| `lix_active_account_id()` | text | Active SQL-session account. |
| `lix_active_branch_id()` | text | Active branch. |
| `lix_active_branch_commit_id()` | text | Active branch head pinned for the statement. |
| `lix_latest_checkpoint_commit_id()` | text | Active branch's latest checkpoint, or the repository root if it has none. |
| `lix_root_commit_id()` | text | Repository bootstrap root. |
| `uuidv7()` | uuid | Generate a UUIDv7 value. |
| `CURRENT_TIMESTAMP` | timestamptz | Transaction-start instant at microsecond precision. |

## JSONB

Cast JSON text or a bound JSON value with `::jsonb` and use PostgreSQL
operators:

```sql
SELECT
  value -> 'primary_key' AS primary_key,
  value ->> 'key' AS schema_key
FROM lix_registered_schema
WHERE value @> '{"deprecated":false}'::jsonb;
```

Supported syntax includes `->`, `->>`, `#>`, `#>>`, `@>`, `?`, equality, and
`'…'::jsonb`. Missing paths return SQL `NULL`; `->` preserves JSONB `null`,
while `->>` converts JSONB `null` to SQL `NULL`. Negative array indexes follow
PostgreSQL behavior.

The SDK accepts structured JSON parameters directly. If a parameter contains
JSON text, cast it explicitly:

```ts
await lix.execute(
  "INSERT INTO acme_event (id, payload) VALUES ($1, $2::jsonb)",
  [id, JSON.stringify(payload)],
);
```

## Branch and history

`lix_active_branch_commit_id()` resolves the same pinned head used by history
functions called without an explicit commit:

```sql
SELECT lixcol_depth, title
FROM lix_history('acme_task')
WHERE id = 't1'
ORDER BY lixcol_depth;
```

`lix_latest_checkpoint_commit_id()` returns the latest checkpoint for the
active branch, not the newest repository-global checkpoint. If the branch has
no checkpoint, it returns `lix_root_commit_id()`. Use both branch-scoped
accessors to read working changes in one query:

```sql
SELECT lixcol_row_pk, diff_type
FROM lix_diff(
  'lix_file',
  lix_latest_checkpoint_commit_id(),
  lix_active_branch_commit_id()
);
```

`lix_state_at(relation, commit_id)` returns the complete tracked state of a
relation at one commit. Its columns are identical to the live relation, and
entities that did not exist at that commit produce no row:

```sql
SELECT id, path, content
FROM lix_state_at('lix_file', $1)
WHERE id IN ($2, $3);
```

The relation argument must be a text literal. The commit may be a text
parameter or `lix_root_commit_id()` / `lix_active_branch_commit_id()`. Primary
key `=` and `IN` predicates are pushed into the point-in-time read, so batched
entity lookups do not scan unrelated tracked rows. Untracked rows are never
included.

The supplied commit is a closed snapshot. A local commit records its exact
global dependency in `lix_commit.base_commit_id`; its physical state is an
immutable local-overlay root plus that pinned immutable global root.
`lix_state_at` composes both roots and returns inherited global rows plus local
values and tombstones, with local intent winning. Global commits have a null
base. The base is a state dependency, not ancestry, so it is deliberately
absent from `parent_commit_ids` and `lix_commit_ancestry()`. `lix_history`
does follow the base dependency so history can explain the complete state.

When global advances, the next live access to a local branch lazily publishes
one metadata-only local commit pinned to the latest global commit. An actual
local write performs the same base advance in its own commit, so multiple
global advances coalesce and global publication remains O(1) rather than
fanning out over every branch. No global rows are copied into the local
overlay. The active commit ID and every live row therefore always describe the
same closed snapshot.

For `lix_history`, depth is the shortest dependency distance using either a
causal parent edge or a state-base edge. `lix_commit_ancestry()` remains
strictly causal-parent-only.

`lix_commit_ancestry()` returns the active head at depth `0` and every
reachable ancestor once at its shortest depth. Pass one commit ID to use an
explicit graph anchor:

```sql
SELECT commit_id, depth
FROM lix_commit_ancestry($1)
ORDER BY depth, commit_id;
```

`lix_restore` is an insert-only command sink:

```sql
INSERT INTO lix_restore (commit_id)
VALUES ($1)
RETURNING commit_id;
```

The commit must exist and be an ancestor of the active branch head. The
command returns the restored commit ID. It creates no commit, leaves other
branches untouched, preserves branch-local untracked rows, and starts a fresh
undo interval. A restore cannot be combined with another write in the same
transaction and must be the final statement before commit or rollback.
Orphaned commits may remain stored until ordinary
reachability-based garbage collection reclaims them. Checkpoint rows remain
stored even when their commits are no longer on the branch.

Use `execute` for remote callers as well; restore does not add a server-protocol
endpoint or a typed SDK method.

## IDs and time

```sql
INSERT INTO event (id, occurred_at)
VALUES (uuidv7(), CURRENT_TIMESTAMP);
```

Bound parameters may use `?` or `$1`, `$2`, and so on, but a statement cannot
mix the two styles.
