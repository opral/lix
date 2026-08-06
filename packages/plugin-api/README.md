# Lix plugin API v1

The canonical hard-cut Component authoring API. Its WIT package is exactly
`lix:plugin@1.0.0`.

## Author contract

A plugin implements five required callbacks: `open`, `file_changed`,
`entities_changed`, `restore`, and `cold_file_changed`. These names match the
variants passed to the one stateful WIT export. The required cold and restore callbacks prevent a plugin
from compiling while silently omitting eviction, restart, history, or reopen
behavior. Stateless conflict resolution has a canonical default.

After implementing `Plugin`, export the component from the plugin crate:

```rust
struct MyPlugin;

impl lix_plugin_api::Plugin for MyPlugin {
    // Implement all five required lifecycle callbacks.
}

lix_plugin_api::export_plugin!(MyPlugin);
```

The export macro is required; a trait implementation alone does not expose the
Component entry point to the host.

Use `Output::entity(EntityMutation)` for ordinary creates, upserts, and deletes.
The SDK owns framing, bounded batching, create separation, counts, and final
flush. This is the only entity output path for every format. State is opaque
byte state; file replacement is streamed through
the same atomic output.

Input entities and output mutations use the same bounded single-section entity
page envelope. Snapshot pages are the universal path. There are no
author-selected representations, per-entity Component calls,
guest-owned cursors, or multi-section pages.

Entity snapshots are compact, duplicate-free, number-free JSON objects with
recursively lexicographically sorted keys. Use strings for numeric domain
values. `Output::entity` batches snapshots automatically.

Cold updates expose the accepted predecessor snapshot, sparse edits, durable
entities, and create context directly. There is no materialization-dependent
source variant.

Plugin-owned lifecycle paths are required strings. The host resolves and
validates them before entering the guest, so plugins do not handle an absent
path branch.

`entities_changed` receives the predecessor path because entity edits cannot
rename a file. `restore` receives only the optional accepted snapshot and
durable entities; its descriptor is already fixed by the host. Conflict
replacement has one meaning—content replacement—so authors choose only
`TakeBase`, `TakeA`, `TakeB`, `Replace`, or `Delete`.

`CreateContext` is opaque and deterministically maps a local reference to the
host-reserved UUID namespace.

## Manifest contract

The manifest requires `key`, `match`, `entry`, and `schemas`. File bytes always
use blob durability. Do not declare `materialization`, `runtime`, or
`api_version`; the host validates the component and durable registry against
`lix:plugin@1.0.0`.

See [the experiment and profiling contract](../../rfcs/universal-plugin-api.md)
for the wire shape, correctness gates, and cross-format measurement matrix.
