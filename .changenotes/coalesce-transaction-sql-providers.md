---
type: patch
---

Transaction SQL reads now construct each DataFusion table provider once by
installing snapshot-backed history providers alongside transaction-overlay
writable providers, instead of constructing and then replacing duplicates.
