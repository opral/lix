---
type: patch
---

Commit-root materialization now resumes from the last durable state root instead of replaying history from genesis.

Closing a rootless replay interval used to prove root availability by re-deriving every ancestor root from the changelog back to the first commit, so the periodic root materialization replayed the entire history and its cost grew quadratically with commit count. Availability is now proven from the commit-state manifest that already owns the root, plus a readable-closure check on the tree it addresses. A damaged root is still never resumed from, so explicit repair stays total.
