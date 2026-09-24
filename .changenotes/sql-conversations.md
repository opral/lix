---
type: minor
---

Added built-in SQL conversations and comments for discussions attached to rows or commits, with standalone conversations supported through a nullable target.
Conversations may also have a nullable human-readable `title`.
Conversations have a `resolved` flag that defaults to `false`; resolve or reopen with an ordinary `UPDATE`, and read who changed it and when from change metadata.

Conversation targets and replies enforce same-scope references and cascade when their parent is deleted, including during merges. Schema tables now support `INSERT … SELECT`, allowing replies to copy their conversation’s scope in one statement. Scope-mismatch errors explain when a referenced target exists globally.
