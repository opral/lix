---
type: patch
---

Re-adding an entity that was deleted in an earlier checkpoint now reports the same `diff_id` as adding an entity for the first time.

Deleting an entity leaves an internal tombstone behind at the next checkpoint, and that tombstone was leaking into the `diff_id` of a later re-add — so two identical "this row is now here" changes could carry different ids depending on the entity's older history. `diff_type`, `before_change_id`, and `after_change_id` are unchanged.
