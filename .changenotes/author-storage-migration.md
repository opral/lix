---
type: minor
---

Repositories using storage format v82 upgrade to v83 on open. The upgrade preserves existing branch controls, HOT rows, and tracked-state history while enabling persisted row author IDs for new writes. Rows written before author IDs were stored report the anonymous account where the author cannot be recovered.
