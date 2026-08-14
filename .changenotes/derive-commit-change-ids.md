---
type: patch
---

Removed the `changelog.commit_change_id` storage space.

A commit's public change id is now derived from its commit id instead of being
generated independently, stored on the commit record, and mirrored into a
reverse-index space. Commit ids already reserve their low 32 bits for packed
change ordinals, and packed ordinals start at one, so the all-zero address is
permanently free and is what the commit's own change now uses. Inverting a
commit change id is arithmetic, so the index has nothing left to hold.

Every commit writes one fewer storage row and one fewer uuid.
