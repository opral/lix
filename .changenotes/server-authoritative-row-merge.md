---
type: minor
---

Resolve concurrent partial-replica edits through the same native row merge pipeline as branch merges.

Registered schema/plugin merge hooks remain active. The default for overlapping
values is incoming-write precedence in server acceptance order, rather than
change-ID ordering. Plugin-managed files serialize the resolved rows; opaque
file content remains atomic. Accepted retries retain their original identity
and cannot overwrite a later server edit by being treated as a new write.

Upgrade SDK and server together for sync protocol 14. The explicit local journal
migration preserves pending edits and existing acknowledgment identities without
resetting browser storage. Opening and resident SQL keep their on-demand and
local execution behavior.

Pending edits are accepted against the server's current branch state even when
other writers advance it during upload. Exact retries keep their original outcome,
already included rows are not applied again, and newer local edits remain pending
for their own acceptance.
