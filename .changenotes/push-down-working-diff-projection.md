---
type: patch
---

`lix_working_diff()` now honors SQL projection when constructing result rows, avoiding diff IDs, row keys, and change-ID strings that an aggregate or narrow query did not request. Empty projections such as `COUNT(*)` also avoid allocating one placeholder row per change.
