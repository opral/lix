---
type: patch
---

Primary-key range predicates now reach storage scans across all file scopes, allowing readers to discard out-of-range rows before payload materialization while preserving local/global visibility and tombstones. Exact file scopes can still use physical range seeks; broader scopes keep the typed range as a per-row predicate.
