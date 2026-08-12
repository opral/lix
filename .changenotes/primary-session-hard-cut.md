---
type: minor
---

Replace workspace sessions with client-owned primary sessions. New repositories
track `lix_default_branch_id` in their initial commit, while `open_lix()` restores
the primary session from untracked client state at
`lix_primary_session_branch_id`. Additional sessions are independent and branch
switches no longer mutate repository state. This is a storage and API hard cut;
older repositories and the removed workspace-session methods are unsupported.
