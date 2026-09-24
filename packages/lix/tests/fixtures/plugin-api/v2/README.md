# Frozen Plugin API v2 archives

These frozen ZIPs were built from the plugin crates during the migration to
`lix:plugin-v2`, based on revision
`852c1151f9fad27b640570c18a2caebbc5e41d55` plus the API identifier changes in
this change. They establish the compiled baseline for the canonical major-only
identifier; they are test artifacts, not published releases.

| Archive | SHA-256 |
| --- | --- |
| plugin_csv.lixplugin | 53a3629b32fbef2b68db45b50c2f4bd4b94d0b97f2bda452581b9672d39740ac |
| plugin_markdown.lixplugin | d17f353e97d8a2705649a445dae71e8a3d0a15a2cf9929627cad16d7c05e6de7 |

Never rebuild these files during normal tests, or replace them when adding host
capabilities. Their imports must remain unchanged to detect regressions in the
promise that newer hosts execute older compiled plugins. Add another fixture
when a new API contract needs coverage; retain the existing fixtures.

`import-subset.wasm` is a small Component compiled from the adjacent WAT using
`@bytecodealliance/jco-transpile/wasm-tools` `parse()`. It imports one host
operation through a core Wasm wrapper, allowing both Wasmtime and the JavaScript
host to verify that adding another host operation preserves existing imports.
Its SHA-256 is
`43c1732117eb3a1deef957c7293b9b6391768a186749f6789ce2c7759a550e8e`.
