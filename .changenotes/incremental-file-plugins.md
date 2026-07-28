---
type: minor
---

Structured files now merge incrementally through the new Component v2 plugin
platform.

Reference plugins for CSV and TSV, JSON, Markdown, Excalidraw, and Git-compatible
text turn localized file edits into sparse semantic changes without reparsing
or rendering the complete document. Concurrent edits merge at the entity level,
and plugin authors can build on the same public Rust API used by the bundled
plugins.
