---
type: patch
---

Fixed several correctness and reliability issues across storage, branches, and files.

This includes stale SlateDB reads, truncated scans, false transaction conflicts, incorrect branch reverts, subquery failures, and directory operations that could leave invalid state.
