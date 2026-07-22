---
type: patch
---

Remote point observations of `SELECT data FROM lix_file` now send localized blob changes as compact prefix/suffix splices.

The first result and large replacements remain complete snapshots. Deltas are used only when they reduce the live event by more than 10%, and reconnects always restart from a complete result.
