---
type: patch
---

Fixed missing results from indexed filters and joins after large bulk writes.

Bulk inserts and replacements now publish index entries alongside their rows,
preventing queries from treating existing records as absent. This fixes translation
compilation falling back to message keys after larger inlang imports.
