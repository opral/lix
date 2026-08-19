# Cedar permission prototype

This plugin projects canonical permission files under `/.lix/permissions/`
into the read-only `cedar_permission_source` row surface. Files remain the only
editable source of truth:

- `schema.cedarschema` defines the repository's entity and action model.
- Any `*.cedar` files form one policy set.
- Optional `entities.cedar.json` defines relationships such as company teams.

Lix core consumes the projection when the plugin is installed and falls back
to the same files directly when it is not. Structured file reads authorize the
session's active account with `Action::"view"` and the file's stable
`File::"<lix_file.id>"` identity. Cedar's default deny therefore keeps every
other file private. Renaming a file does not invalidate a publication.

`lix_permission_grant` is the core-owned standard sharing model. Every grant is
a tracked global row on `GLOBAL_BRANCH_ID`, inherited into every working
branch. Its principal is an account, group, or anonymous; its access level is
viewer, commenter, contributor, editor, or manager; and its resource is one of
repository, directory, file, table, or row. File-scoped database identities are
represented as `Table(schema_key, file_id)` and
`Row(schema_key, file_id, row_pk)`.

The generic authorization API uses compact JSON tuples as canonical Cedar
entity IDs: `[schema_key,file_id]` for `Table` and
`[schema_key,file_id,row_pk]` for `Row`. The `row_pk` component is itself the
canonical Lix primary-key tuple.

The default adapter turns applicable grants into Cedar permits. Repository
policy files remain part of the same policy set, so custom permits can add
company-specific behavior and Cedar `forbid` policies override a standard
grant. A repository can also ignore the standard model by storing no grant
rows and expressing its authorization entirely in Cedar files.

The source projection deliberately remains `cedar_permission_source`, not a
`lix_*` table: that namespace is reserved for trusted core schemas, while the
projection is plugin-owned and read-only.

## Prototype boundaries

- Anonymous SQL is disabled by the Lixray integration; arbitrary SQL is not
  policy-filtered.
- Policy compilation is not cached yet.
- Core links only `cedar-policy-core` 4.2.2. The full Cedar validator enables
  `serde_json/preserve_order` transitively, changing Lix's canonical JSON
  representation through Cargo feature unification; it remains isolated in
  this WASI plugin.
- Policy loading and the protected file read are not yet one storage snapshot.
- Grant and policy edits are not yet guarded by `Action::"managePolicies"`.
- The plugin validates individual source files. Core parses the complete policy
  set before using it, but cross-file static validation against the schema is
  not implemented in this prototype.
