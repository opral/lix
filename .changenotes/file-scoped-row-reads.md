---
type: patch
---

Reading plugin rows by `lixcol_file_id` now seeks straight to that file instead of scanning the whole schema.

`SELECT ... WHERE lixcol_file_id = ?` previously matched the file only after every row of the schema had been read, so a narrow read of one file grew with the size of the branch. The file scope is now part of the scan itself, and the cost tracks the number of rows in the file.
