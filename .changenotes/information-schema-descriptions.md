---
type: minor
---

`information_schema.columns` and `information_schema.lix_surfaces` now carry a `description`.

A registered schema's `description` annotations for its table and columns are what tools have always been able to read from the schema document; now they are one column away in the catalog itself, next to the type and nullability. The composed views (`lix_file`, `lix_directory`, `lix_branch`, `lix_change`), the `lixcol_*` bookkeeping columns, and the built-in file, directory, and key-value schemas that shipped without descriptions describe themselves the same way. The column is NULL where nothing was written.
