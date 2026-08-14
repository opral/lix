# Writing Lix plugins

Plugin authors depend on the same `lix` crate as engine users:

```toml
[dependencies]
lix = "0.11"
```

Compile the plugin for the Component Model target:

```sh
cargo build --release --target wasm32-wasip2
```

The build selects Lix's small plugin-authoring surface automatically; it does
not compile the repository engine, SQL stack, storage machinery, or default
Wasm runtime. The WIT package is `lix:plugin@1.0.0`.

## Author contract

A plugin implements five required callbacks: `open`, `file_changed`,
`rows_changed`, `restore`, and `cold_file_changed`. These names match the
variants passed to the one stateful WIT export. The required cold and restore callbacks prevent a plugin
from compiling while silently omitting eviction, restart, history, or reopen
behavior. Stateless conflict resolution has a canonical default.

After implementing `Plugin`, export the component from the plugin crate. A
complete minimal implementation is available in
[`examples/plugin_minimal.rs`](examples/plugin_minimal.rs):

```rust
struct MyPlugin;

impl lix::plugin::Plugin for MyPlugin {
    // Implement open, file_changed, rows_changed, restore, and
    // cold_file_changed. Conflict resolution has a default.
}

lix::plugin::export!(MyPlugin);
```

The export macro is required; a trait implementation alone does not expose the
Component entry point to the host.

Use `Output::row(RowMutation)` for ordinary creates, upserts, and deletes.
The SDK owns framing, bounded batching, create separation, counts, and final
flush. This is the only row output path for every format. State is opaque
byte state; file replacement is streamed through
the same atomic output.

Input rows and output mutations use the same bounded single-section row
page envelope. Snapshot pages are the universal path. There are no
author-selected representations, per-row Component calls,
guest-owned cursors, or multi-section pages.

Row snapshots are compact, duplicate-free, number-free JSON objects with
recursively lexicographically sorted keys. Use strings for numeric domain
values. `Output::row` batches snapshots automatically.

Cold updates expose the accepted predecessor snapshot, sparse edits, durable
rows, and create context directly. There is no materialization-dependent
source variant.

Plugin-owned lifecycle paths are required strings. The host resolves and
validates them before entering the guest, so plugins do not handle an absent
path branch.

`rows_changed` receives the predecessor path because row edits cannot
rename a file. `restore` receives only the optional accepted snapshot and
durable rows; its descriptor is already fixed by the host. Conflict
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

## Installing a plugin

Installing a plugin is a normal tracked repository file write. Write the
`.lixplugin` archive to its canonical path:

```text
/.lix/plugins/<plugin-key>.lixplugin
```

The archive contains `manifest.json`, the schemas declared by that manifest,
and the compiled Component as `plugin.wasm`. For example:

```sh
cp target/wasm32-wasip2/release/my_plugin.wasm plugin.wasm
zip -0 my-plugin.lixplugin manifest.json schemas/*.json plugin.wasm
```

Updating that file replaces the plugin, and deleting it uninstalls the plugin.
Lix validates the archive and updates its derived registry and schema state in
the same transaction. There is intentionally no separate `install_plugin` API.
