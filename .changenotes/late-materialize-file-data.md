---
type: patch
---

Deferred unchanged `lix_file.data` projections until after DataFusion selects the final metadata rows.

Metadata filters, ordering, and limits now avoid loading and copying discarded file bytes through Arrow while preserving the general SQL fallback for expressions that consume file data.
