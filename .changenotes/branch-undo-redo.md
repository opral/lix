---
type: minor
---

Added persistent undo and redo for tracked branch history across the Rust SDK, JavaScript SDK, remote protocol, and CLI.

Undo and redo append inverse and replay commits without rewinding branch history. Atomic batches and transactions remain one undo unit, while untracked state remains unchanged; checkpoints and merge commits form undo boundaries.

The linear-history path reads only the target commit's changed identities instead of diffing whole branch states. Repeated edits remain independent of total branch depth; resolving a before-image can still depend on the distance to that identity's prior revision. Complex filesystem cascades retain the general historical-diff fallback.
