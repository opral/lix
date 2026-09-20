---
type: minor
---

Checkpoint status now has one public home: `lix_log().is_checkpoint`, evaluated at the query anchor.

Remove `lix_commit.is_checkpoint`, `lix_log().is_checkpoint_active`, and `lix_history().lixcol_commit_is_checkpoint`. Join the log and row history on destination commit ID at the same anchor to query changes from active checkpoints. Undo and redo signatures are unchanged; stored commits remain immutable. Existing repositories retain their checkpoint identities and upgrade through the supported migration path.
