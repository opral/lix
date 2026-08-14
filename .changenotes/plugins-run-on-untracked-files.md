---
type: minor
---

Plugins now run on untracked files, so an untracked file's contents are queryable and editable as rows.

Previously plugin reconciliation was skipped for untracked writes: an untracked JSON file was stored as a descriptor plus an opaque content blob with no rows at all, and none of its contents could be queried. A file's rows now follow the file's own lane, so the same file behaves the same way whether it is tracked or untracked — untracked rows carrying a change id and no commit id, exactly as other untracked state does. Editing one of those rows re-renders the file's bytes on both lanes alike.

Untracked files intentionally receive no durable plugin checkpoint, because a checkpoint is a persisted accelerator and untracked state is defined not to be durable. A large untracked file is therefore re-parsed each time the repository is opened.

Untracked change ids identify a row within the session that wrote it. They are not stable across sessions and should not be compared between them.
