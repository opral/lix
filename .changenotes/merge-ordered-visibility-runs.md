---
type: patch
---

Reduced redundant allocation and scan time across local and global state.

Proven ordered runs now merge directly while resolving shadowed rows and tombstones, avoiding a full concatenation and sort of every candidate row.

Read-only transactions forward their original scan request when the committed reader already resolves visibility and the transaction proves it has no staged rows, tombstones, or collection replacements. This avoids a second full visibility pass and preserves bounded scan requests.

Large local batches with a bounded global overlay reuse their row storage, append only global winners, and merge in place while preserving row payload and provenance sidecars.
