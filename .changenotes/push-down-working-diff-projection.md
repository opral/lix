---
type: patch
---

Relation-specific `lix_diff()` queries honor SQL projection when constructing result rows, avoiding row identities, changed payloads, and file descriptors that an aggregate or narrow query did not request. Empty projections such as `COUNT(*)` also avoid allocating one placeholder row per change.
