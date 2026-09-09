---
type: minor
---

Removed the unused `BranchEquals` storage precondition (`branchEquals` in JavaScript).

Custom storage adapters only need space-aware preconditions. Use `KeyValueEquals` to condition a write on an exact stored value.
