---
type: minor
---

Git replay can now seed the complete parent tree for a bounded commit window.

Use `--parent-tree full` when untouched parent files must remain available in current and historical snapshots; the default window-scoped mode remains unchanged.
