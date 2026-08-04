---
type: patch
---

Bounded first publication of columnar current state on long commit histories.

Lix now authenticates cumulative touched schema families in each commit-state
manifest, eliminating history-length manifest walks while preserving
fail-closed behavior for broad, selected-source, and merge lineages.
