# plugin-csv

Rust/WASM component CSV plugin for the Lix engine.

## Current scope

- Provides the crate, manifest, and schema scaffolding for a row-level CSV plugin.
- Uses `packages/engine/wit/lix-plugin.wit` as the API contract.
- `detect-changes` parses CSV/TSV input into table and row snapshots.
- `render` rebuilds canonical CSV bytes from the latest active-state projection,
  sorting rows by their fractional row order key.
