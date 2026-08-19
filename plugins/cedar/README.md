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

The projection does not use a `lix_*` key because that namespace is reserved
for trusted core schemas. Promoting this prototype to a built-in
`lix_cedar_*` surface requires either a core-owned projection or an explicit
trusted-plugin registration path.

## Prototype boundaries

- Anonymous SQL is disabled by the Lixray integration; arbitrary SQL is not
  policy-filtered.
- Policy compilation is not cached yet.
- Core links only `cedar-policy-core` 4.2.2. The full Cedar validator enables
  `serde_json/preserve_order` transitively, changing Lix's canonical JSON
  representation through Cargo feature unification; it remains isolated in
  this WASI plugin.
- Policy loading and the protected file read are not yet one storage snapshot.
- Policy edits are not yet guarded by `Action::"managePolicies"`.
- The plugin validates individual source files. Core parses the complete policy
  set before using it, but cross-file static validation against the schema is
  not implemented in this prototype.
