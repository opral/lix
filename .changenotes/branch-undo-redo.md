---
type: minor
---

Added persistent undo and redo for tracked branch history across the Rust SDK, JavaScript SDK, remote protocol, and CLI.

Undo and redo append inverse and replay commits without rewinding branch history. Atomic batches and transactions remain one undo unit, while untracked state remains unchanged; checkpoints and merge commits form undo boundaries.
