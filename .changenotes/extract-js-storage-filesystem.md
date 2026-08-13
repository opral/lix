---
type: minor
---
Move filesystem storage from `@lix-js/sdk` to the independently versioned
`@lix-js/storage-filesystem` package and rename it to `FilesystemStorage`. Rust
now uses the single `FilesystemStorage::new(path)…open()` configuration path;
JavaScript uses `new FilesystemStorage({ path })`.

Repository metadata now always lives at `<path>/.lix`; the external `lixDir`
option and all previous `LocalFilesystem` entry points are removed without
compatibility aliases.
