---
type: patch
target: lix
---

Fixed automatic upgrades of browser repositories with cached query indexes and pending local edits.

Upgrades now preserve resident indexes whose format is unchanged, while keeping strict checks that repository content and local recovery data survive migration. Preservation reports also account for historical metadata upgrades from storage version 86.
