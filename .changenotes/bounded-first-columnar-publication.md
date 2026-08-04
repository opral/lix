---
type: patch
---

Bounded first publication of columnar current state on long commit histories.

Lix now authenticates cumulative touched schema families in each commit-state
manifest and carries that bounded absence authority across linear, merged, and
selected-source lineages. Mutation scopes that cannot be bounded exactly still
fail closed.
