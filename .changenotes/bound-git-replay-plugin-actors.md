---
type: patch
---

Git history replay can now ingest large atomic text commits without exhausting the bounded WebAssembly actor cache.

Replay keeps durable Git-text rows and materialization proofs while retiring one-shot file actors immediately after their output is staged.
