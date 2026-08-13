---
type: patch
---

Commit no longer re-reads the whole repository state to decide where a commit-root replay may resume.

Closing a rootless replay interval checked that the previous durable state root was readable by traversing every row of the tree it addresses. That check cost time proportional to total state size and ran once per replay interval, so commit still carried a quadratic term even after replay itself was bounded. The commit path now proves only that the resume point is addressable, which is what actually distinguishes a usable root from a damaged one; the completeness of the chunk closure is already guaranteed by atomic publication and by garbage collection reaching chunks from refs. Explicit repair (`rebuild_tracked_state_for_branch`) still proves the whole closure, so a damaged root is still never resumed from during repair and repair stays total.
