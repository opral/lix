---
type: minor
target: lix
---
Make row references identify an exact file scope as well as a relation and typed primary key. Diff, checkpoint, and recovery selections distinguish equal keys in different files.

Breaking: `lix_row_ref(relation, file_id, pk_1, ...)` now requires the nullable file argument. Use NULL for fileless rows and the public `lix_file`/`lix_directory` relations. Encoded references use the new v2 format; v1 values are rejected. Branch remains implicit in the operation context.
