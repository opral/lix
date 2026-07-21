---
type: major
---

Tracked-state roots no longer store derived `lix_commit` rows.

Commit rows are synthesized from `changelog.commit`, leaving immutable state
trees to contain only authored changes. This makes ordinary one-row commits use
the tree's singleton path-copy path and substantially reduces write
amplification. Commit-root metadata now carries a backend-neutral format marker;
repositories written with the previous layout are rejected and must be
recreated instead of being silently inherited.
