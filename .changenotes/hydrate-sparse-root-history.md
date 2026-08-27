---
type: patch
---

Root and latest-checkpoint queries now hydrate deferred commit history on sparse sync replicas.

Observers and diff commands transparently retry after fetching a missing commit-graph ancestor instead of failing with an internal error.
