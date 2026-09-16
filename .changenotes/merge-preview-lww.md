---
type: minor
---

Removed the unused `conflicts` field and conflict types from branch merge previews.

Lix continues to combine independent column edits and resolve competing edits
with last-writer-wins or a plugin merger. Applications should review the changes
rather than use a conflict array as an approval gate. Server protocol version 11
marks the response change; clients and servers must use compatible versions.
