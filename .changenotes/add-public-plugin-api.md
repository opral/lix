---
type: minor
---

Added `lix_plugin_api_v2`, the public Rust authoring package for Component-v2
plugins. Plugin authors implement four typed semantic transitions and may
override one deterministic, stateless entity-conflict hook while the package
owns WIT resources, bounded packet paging, sparse attachments, zero-copy
conflict selections, and retry-stable ID encoding. The CSV, JSON, Markdown,
and Excalidraw reference plugins now use the same public surface.
