---
type: minor
---

`FsBackend` now requires an explicit `syncAllFiles` option and supports on-demand file sync.

`new FsBackend({ path, syncAllFiles: true })` syncs the full workspace as before. With `syncAllFiles: false`, the lix opens without workspace files and `lix.importFilesystemPaths(["notes/today.md"])` syncs selected files on demand. Imported paths are exact workspace-relative file paths, not directories or globs. In Rust, use `FsBackendOpenOptions::new(root, sync_all_files)` and `import_paths()`.
