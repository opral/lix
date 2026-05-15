# Lix Plugin API MVP

Prototype only. Lix `v0.6` is the embed-ready core release; plugin API work is for `v0.7`.

## What A Plugin Does

On `lix_file` byte writes:

```text
file bytes -> plugin.detect_changes -> semantic Lix rows
```

For Sem, the MVP goal is simply: parse a file and make Sem entities queryable in Lix.

## Archive

A plugin is a zip:

```text
manifest.json
plugin.wasm
schema/<schema>.json
```

Minimal manifest:

```json
{
  "key": "sem_plugin",
  "runtime": "wasm-component-v1",
  "api_version": "0.1.0",
  "match": { "path_glob": "*.js" },
  "entry": "plugin.wasm",
  "schemas": ["schema/sem_entity.json"]
}
```

Plugins may only emit rows for schemas listed in `schemas`.

## Install

```rust
lix.register_plugin(RegisterPluginOptions {
    bytes: plugin_archive_bytes,
}).await?;
```

Archives are stored as tracked, version-local files:

```text
/.lix_system/plugins/<key>.lixplugin
```

## Runtime

Embedders pass a Wasm runtime:

```rust
open_lix(OpenLixOptions {
    wasm_runtime: Some(Arc::new(runtime)),
    ..Default::default()
}).await?;
```

Without a runtime, plugins can be installed but not executed.

## WIT Shape

See:

```text
packages/engine/wit/lix-plugin.wit
```

MVP export:

```wit
detect-changes: func(
  before: option<file>,
  after: file,
  state-context: option<detect-state-context>
) -> result<list<entity-change>, plugin-error>;
```

Return rows like:

```wit
record entity-change {
  entity-id: string,
  schema-key: string,
  snapshot-content: option<string>,
}
```

`snapshot-content` is JSON text. `none` means tombstone.

## Examples

Fast SDK plumbing test:

```text
packages/rs-sdk/tests/plugin.rs
```

Real Wasmtime component test:

```text
packages/engine/tests/plugin/detect_changes.rs
```

Current limitation: path-only file renames do not trigger plugin reconciliation yet.
