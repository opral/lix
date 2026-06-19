---
type: minor
---

Simplified filesystem-backed Lix storage around `FsBackend`.

Use `new FsBackend({ path: "./workspace" })` for persistent filesystem-backed storage, or `new FsBackend({ path: "./workspace", storage: "memory" })` when the workspace should be synced into an in-memory Lix without writing `.lix` state back to disk. Filesystem sync can now be limited to exact workspace-relative paths with `filter.includePaths`.
