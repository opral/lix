---
type: patch
---

Fixed plugin upgrades rejecting compatible schema amendments, including documentation changes and supported column additions.

Upgrades now follow the same schema amendment rules as other schema updates, preserve readable existing rows, and persist added column defaults. Incompatible changes and conflicting definitions shared by different plugins remain rejected.
