---
type: patch
---

Re-adding a row that was deleted in an earlier checkpoint now reports the same `diff_id` as adding a row for the first time.

Deleting a row leaves an internal tombstone behind at the next checkpoint, and that tombstone was leaking into the `diff_id` of a later re-add — so two identical "this row is now here" changes could carry different ids depending on the row's older history. `diff_type`, `before_change_id`, and `after_change_id` are unchanged.
