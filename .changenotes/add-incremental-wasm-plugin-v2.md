---
type: minor
---

Added the production `wasm-component-v2` plugin runtime and an incremental CSV plugin. Ordinary SQL blob edits now preserve validated splice provenance, keep one failure-isolated document actor per file, emit sparse semantic changes, and reuse the committed materialization without a full filesystem render.

The v2 runtime enforces bounded paged inputs and outputs, stable retry-scoped IDs, exact session observations, and the production 64 MiB guest-memory limit.
