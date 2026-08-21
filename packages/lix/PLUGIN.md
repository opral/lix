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
not compile the repository engine, query stack, storage machinery, or default
Wasm runtime. The WIT package is `lix:plugin@2.0.0`.

## Author contract

Plugins expose independent capabilities. Most schemas need no executable code:
Lix merges concurrent typed rows with host-native column-based LWW. A plugin
implements `ColumnMerger` only when one overlapping column needs a domain merge,
such as composing disjoint edits inside a large Markdown string.

A file format implements `FileProjection`. Its four required methods describe
one bytes-to-rows projection:

- `parse`: complete file bytes to complete rows;
- `parse_changes`: sparse file edits to row mutations;
- `serialize`: complete rows to complete file bytes;
- `serialize_changes`: row mutations to sparse file edits.

There is no separate file merge API. Lix merges the rows first, then asks the
projection to serialize the accepted row changes. Cold `parse_changes`
receives durable rows when identities must be recovered; warm incremental
parsing does not hydrate untouched rows.

After implementing the capabilities, export exactly those capabilities. A
complete minimal row-only merger is available in
[`examples/plugin_minimal.rs`](examples/plugin_minimal.rs):

```rust
struct ExampleColumnMerger;

impl lix::plugin::ColumnMerger for ExampleColumnMerger {
    // Called only when both sides changed the same column differently.
}

lix::plugin::export_capabilities! {
    column_merger: ExampleColumnMerger,
}
```

CSV and Markdown demonstrate the combined capability shape; JSON, Excalidraw,
and text demonstrate projection-only plugins. Row-only merger integration is
covered by an explicitly test-only E2E component.

The export macro is required. Export presence is the capability declaration;
there are no manifest capability flags and no disabled placeholder exports.

Use `RowOutput` and `RowChangeOutput` for rows. Use `FileOutput` and
`FileEditOutput` for bytes. The scoped outputs prevent a parse operation from
writing bytes or a serialize operation from inventing rows. The SDK owns
framing, bounded batching, attachments, state, and final flush.

Input rows and output mutations use the same bounded single-section typed-row
page envelope. Native Schema v1 pages are the universal path. There are no
author-selected representations, per-row Component calls,
guest-owned cursors, or multi-section pages.

Rows are schema-keyed native `lix-schema` values carrying the exact schema
fingerprint. JSON-shaped domain values use `jsonb`; they are never encoded as
text or wrapped in an outer JSON row object. `RowOutput` batches typed rows and
page-local attachments automatically.

For a `ColumnMerge`, `a` and `b` are ordered by the durable conflict rank;
`b` is the host LWW result. Returning `UseLww` keeps it. Returning
`Replace(OwnedColumnValue)` replaces only that column. The merger also receives
lazy complete base/a/b rows for structural checks, but it cannot change row
identity or resolve creation/deletion races.

`CreateContext` is opaque and deterministically maps a local reference to the
host-reserved UUID namespace. `id(local_ref)` returns a native `uuid::Uuid`.

`Snapshot` is the single immutable source for accepted file bytes and private
plugin state. Its ranged reads preserve the snapshot resource semantics across
parse and serialize operations; the SDK does not expose file-specific or
projection-specific aliases.

## Manifest contract

The manifest requires `key` and `schemas`. `entry` is present only when the
archive contains executable capabilities. `file_match` is present exactly when
the component exports `FileProjection`; a row-only `ColumnMerger` has no file
matcher. The legacy `match` key is rejected. Do not declare capability flags,
`materialization`, `runtime`, or `api_version`; the host validates the component
and durable registry against `lix:plugin@2.0.0`.

See [the v2 design and profiling contract](../../rfcs/universal-plugin-api.md)
for the wire shape, correctness gates, and cross-format measurement matrix.

## Installing a plugin

Installing a plugin is a normal tracked repository file write. Write the
`.lixplugin` archive to its canonical path:

```text
/.lix/plugins/<plugin-key>.lixplugin
```

The archive contains `manifest.json`, the declared schemas, and—when it exports
an executable capability—the compiled Component as `plugin.wasm`. For example:

```sh
cp target/wasm32-wasip2/release/my_plugin.wasm plugin.wasm
zip -0 my-plugin.lixplugin manifest.json schemas/*.json plugin.wasm
```

Updating that file replaces the plugin, and deleting it uninstalls the plugin.
Lix validates the archive and updates its derived registry and schema state in
the same transaction. There is intentionally no separate `install_plugin` API.
