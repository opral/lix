---
type: minor
---

Concurrent whole-file writes now merge plugin-backed files at the plugin entity boundary.

Each session tracks the file state it has actually received. A later blob write applies only that session's semantic additions, edits, and deletions, preserving unseen entities written by other sessions while resolving same-entity races by last write wins. Files without a matching plugin continue to behave as one raw blob entity.

Blind plugin-backed writes use current semantic state to preserve entity identity, but omitted entities are not deleted until the session has received or submitted them.

Session bases are tied to the durable plugin-owner incarnation, so a plugin-to-raw-to-plugin transition cannot revive stale delete authority.
