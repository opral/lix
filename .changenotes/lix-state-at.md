---
type: minor
---

Added immutable, branch-scoped point-in-time reads with `lix_state_at(relation, commit_id)`.

Diff machinery columns now use the reserved `lixcol_` namespace, and repository format v74 adds the authenticated row-primary-key index used for bounded historical reads. Existing repositories require the explicit offline migration before opening.
