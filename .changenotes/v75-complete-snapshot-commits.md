---
type: minor
---

Repository format v75 makes every commit a complete state snapshot: `lix_commit.base_commit_id` names the exact global commit whose state composes beneath a local commit's overlay, so branch-scoped and point-in-time reads (`lix_state_at`) are exact rather than replay-derived.

Existing repositories require the explicit offline migration (`migrate_lix`) before opening. The migration upgrades v72–v74 repositories in place — inferring each local commit's base chronologically, repairing filesystem trees that v72-era partial checkpoints left without their ancestor directories, and fencing every step so an interruption is either cleanly retryable or refused by older engines. Repositories below v72, or repositories whose commit timestamps contradict the chronological inference, are rejected with an explicit error instead of migrating on guessed history.
