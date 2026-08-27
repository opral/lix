---
type: minor
---

Standardized relation payload column names in `lix_diff()`.

Diff queries now use `diff_type` and `row_count`; `lixcol_diff_type` and
`lixcol_row_count` were renamed without compatibility aliases. The `lixcol_`
prefix remains reserved for engine-owned system metadata.
