---
type: patch
---

The Git text WASM plugin now writes base64 content directly into its final JSON
snapshot buffer, avoiding a duplicate large allocation for minified files.
WASM Stores retain a bounded 128 MiB ceiling so warm updates of large minified
documents can materialize their successor without exhausting linear memory.
