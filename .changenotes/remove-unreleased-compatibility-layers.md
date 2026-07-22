---
type: minor
---

Removed unreleased storage compatibility APIs and format fallbacks.

SlateDB callers must use `SlateDB::open()` or
`SlateDB::open_object_store_with_options()`. Legacy namespaced CLI snapshots,
and change records without origin keys are no longer accepted.
