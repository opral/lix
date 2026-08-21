---
type: patch
---

Indexed equality reads now include rows written with `lixcol_untracked = true`, fixing point-filtered joins that could previously return null-extended children even though an unfiltered join found them.
