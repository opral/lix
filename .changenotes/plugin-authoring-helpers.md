---
type: minor
---

Simplified plugin development with native lifecycle testing, validated file edits, and scoped state cleanup.

Plugin authors can test projection hooks without compiling WebAssembly, read edited file ranges without rebuilding the whole file, and clear related private state in one operation. Markdown and CSV use the shared helpers to reduce duplicated edit and cache bookkeeping. Rebuild plugin components against the updated SDK to use the new state cleanup host operation.
