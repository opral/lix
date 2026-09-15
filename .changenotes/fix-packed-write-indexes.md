---
type: patch
---

Fixed missing results from indexed filters and joins after large bulk writes.

Bulk inserts and replacements now publish index entries alongside their rows,
preventing queries from treating existing records as absent. This fixes translation
compilation falling back to message keys after larger inlang imports. Databases
with older, potentially incomplete indexes use a scan when index completeness
cannot be established.

Also fixed nullable primitive projections and compatible schema amendments:
existing rows remain readable, inherited rows retain correct indexed results,
and newly added literal and generated defaults are stored once in the amendment
transaction.
Historical queries preserve the original row values.
