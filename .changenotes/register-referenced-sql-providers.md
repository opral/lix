---
type: patch
---

Read queries now construct only the snapshot-local DataFusion table providers
referenced by their parsed SQL, while catalog-wide introspection retains the
complete provider set.
