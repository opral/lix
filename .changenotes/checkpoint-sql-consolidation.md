---
type: minor
---
Consolidate checkpoint storage and version-control SQL with a breaking repository migration.

Checkpoint membership is immutable `lix_commit.is_checkpoint`; remove `lix_checkpoint` marker writes. Add first-parent `lix_log`, redefine `lix_history` as endpoint changes with checkpoint flags, and rename `lix_state_at` to `lix_as_of`. Working diffs expose their actual endpoints and branches expose `working_base_commit_id`. Replace the latest-checkpoint scalar with filtered logs or the working baseline, according to the query's purpose. Upgrade clients and synchronization peers together.
