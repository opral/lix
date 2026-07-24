---
type: minor
---

Added the production `wasm-component-v2` plugin runtime with opt-in reference
plugins for incremental CSV/TSV, recursive JSON, Markdown, and Excalidraw.
Ordinary SQL blob edits now preserve validated splice provenance, keep one
failure-isolated document actor per file, emit sparse semantic changes, and
reuse the committed materialization without a full filesystem render.
File-scoped semantic SQL edits take the reverse incremental path; statements
chain privately within a transaction and publish once on commit.

The v2 runtime enforces bounded paged inputs and outputs, mutation-scoped IDs,
exact session observations, and a configurable guest linear-memory ceiling.
The same explicit mutation identity reproduces its namespace; transport replay
requires a separate protocol. The current host policy preserves the existing
64 MiB v1 default and uses a configurable 128 MiB per v2 actor, retaining at
most four idle/warm file actors by default. Live transactions remain
individually capped but are not a workspace-wide concurrency cap. Remote splice
bases remain bounded to 16 MiB per session and share a configurable 128 MiB
workspace cache budget; cache saturation falls back to complete-blob retry
without changing results.
