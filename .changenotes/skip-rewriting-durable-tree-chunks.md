---
type: patch
---

Commits no longer rewrite tracked-state tree nodes that are already stored.

The tracked-state tree is content-addressed, so a node's identity is its content. Rebuilding a commit root could still re-write nodes that were byte-for-byte already on disk, and neither storage backend filtered those out. Writers now check first and stage only genuinely new nodes, cutting redundant write traffic in long histories by roughly three quarters without changing on-disk size or query results.
