---
type: minor
---

Added built-in SQL conversations and comments for discussions attached to rows or commits, with standalone conversations supported through a nullable target.
Conversations may also have a nullable human-readable `title`.
Conversations have a `resolved` flag that defaults to `false`; resolve or reopen with an ordinary `UPDATE`, and read who changed it and when from change metadata.

A conversation's `target` is a `ROW_REF` that must name an existing row in the conversation's scope when it is written. Deleting the target, including during a merge or plugin change, leaves the conversation and its `target` unchanged: the conversation is detached while the reference does not resolve, and attached again if the target returns. Deleting a conversation cascades its comments. Schema tables now support `INSERT … SELECT`, allowing replies to copy their conversation’s scope in one statement. Scope-mismatch errors explain when a referenced target exists globally.
