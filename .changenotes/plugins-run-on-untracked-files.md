---
type: minor
---

Plugins now run on untracked files, so an untracked file's contents are queryable as entity rows.

Previously plugin reconciliation was skipped for untracked writes: an untracked JSON file was stored as a descriptor plus an opaque content blob with no entity rows at all, and none of its contents could be queried. A file's entity rows now follow the file's own lane, so the same file produces the same rows whether it is tracked or untracked — untracked rows carrying a change id and no commit id, exactly as other untracked state does.

Untracked files intentionally receive no durable plugin checkpoint, because a checkpoint is a persisted artifact and untracked state is defined not to be durable. A large untracked file is therefore re-parsed each time the workspace is opened.

Untracked change ids identify a row within the session that wrote it. They are not stable across sessions and should not be compared between them.
