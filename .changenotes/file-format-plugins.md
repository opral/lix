---
type: minor
---

Added file format plugins: CSV, Markdown, and plain text files are stored as queryable state instead of blobs.

Writing a file with a matching plugin stores the changes inside the file as entity state. A CSV cell edit is one row-level change that can be queried, diffed, and merged. Reorders are detected: a moved row or paragraph is recorded as a move, not a delete plus an insert. Files without a plugin keep content-defined chunked blob storage.
