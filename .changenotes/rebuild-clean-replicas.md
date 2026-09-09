---
type: patch
---

Rebuild supported clean local replicas from the server during format upgrades,
avoiding historical migrations over sparse downloaded data. The replacement is
prepared and validated in a separate epoch before publication. Replicas with
pending local work or an unproven recovery state are preserved and report
`LIX_ERROR_REPLICA_UPGRADE_BLOCKED` instead of being reset.
