---
type: minor
---

Typed SQL history now routes exact public primary-key predicates in declared `x-lix-primary-key` order and keeps primary-key columns non-null on deletion rows, including nested identity roots.

The relation-blind `LIX_HISTORY_NON_IDENTITY_FILTER` notice has been removed. File and directory history filters retain ordinary SQL row semantics: filtering by a historical path returns revisions with that path, while filtering by immutable identity returns the entity lineage.
