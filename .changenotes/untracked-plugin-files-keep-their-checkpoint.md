---
type: patch
---

Untracked plugin files no longer re-parse from scratch on every session.

An untracked file that a plugin understands now keeps the same durable actor checkpoint a tracked file keeps, so reopening a repository restores it instead of parsing the whole file again. The checkpoint stays a pure accelerator: it is validated against the file's current content and is discarded automatically whenever it no longer matches, and it disappears with the file or its branch.
