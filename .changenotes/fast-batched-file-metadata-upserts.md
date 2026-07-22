---
type: patch
---

Improved the speed of batched `lix_file` path, data, and metadata upserts.

Common upload statements that replace both file bytes and row metadata now use the native bound-write path while preserving descriptor identity, blob replacement, and plugin reconciliation.
