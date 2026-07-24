---
type: minor
---

Lix SQL now exposes a smaller, application-oriented catalog.

The generic `lix_state`, `lix_state_by_branch`, and `lix_state_history` tables
have been removed. Query and mutate registered schemas through their generated
typed tables, use `<schema>_by_branch` for cross-branch state,
`<schema>_history` for branch-reachable entity history, and `lix_change` for
workspace-wide activity.

The storage-level `lix_file_descriptor*`, `lix_directory_descriptor*`, and
`lix_binary_blob_ref*` tables are no longer public. Use the logical
`lix_file*` and `lix_directory*` surfaces instead.

`lix_key_value*` and `lix_registered_schema*` remain public. Internal schemas,
including the hidden filesystem schemas, remain discoverable through
`lix_registered_schema` for application interoperability.

Runtime registration now rejects `x-lix-key: "lix"` and every key beginning
with `lix_`; their base or generated SQL names occupy the namespace reserved
for schemas bootstrapped by Lix. Application and plugin schemas must use an
owner prefix such as `acme_task`. Catalog loading hard-fails for workspaces
that already contain a custom key in this namespace; this release has no
in-engine schema-rename path, so those workspaces require
application-specific export or migration tooling before upgrading.
