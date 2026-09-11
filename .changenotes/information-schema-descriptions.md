---
type: minor
---

`information_schema.columns` and `information_schema.lix_surfaces` now carry a `description`.

A registered schema's `description` annotations for its table and columns are what tools have always been able to read from the schema document; now they are one column away in the catalog itself, next to the type and nullability. The composed views (`lix_file`, `lix_directory`, `lix_branch`, `lix_change`) and the `lixcol_*` bookkeeping columns describe themselves the same way, and the built-in file, directory, and key-value schemas gained the descriptions they lacked. The column is NULL where nothing was written.
