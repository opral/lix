---
type: patch
target: lix
---

Fix first offline file edits in prepared native partial replicas and cold file/directory reads in repositories with installed plugins.

Partial reads now retain the selected file and its required native dependencies. Incoming synchronization prepares retained moving scopes before exposing the new serving generation, so an already prepared document can continue reading and editing locally. Format upgrades preserve pending conversion journals and stop with recoverable diagnostics when local work cannot be safely converted.
