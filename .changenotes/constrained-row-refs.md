---
type: minor
target: lix
---
Add schema-level `row_refs` constraints for text columns containing complete row references. Validate targets against stored rows and pending writes in the current branch, including cross-file references. Support `on_delete: "cascade"` and default `no_action` through ordinary writes, merges, and selective checkpoint/recovery operations, with indexed incoming-reference lookup.
