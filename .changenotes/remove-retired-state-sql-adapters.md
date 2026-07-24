---
type: patch
---

Remove the retired internal SQL adapters for `lix_state`,
`lix_state_by_branch`, and `lix_state_history`. Tests for shared SQL behavior
now exercise typed entity, filesystem, and schema-specific history surfaces
directly.
