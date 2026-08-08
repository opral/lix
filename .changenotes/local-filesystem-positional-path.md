---
type: minor
---

Simplified `LocalFilesystem` to take a workspace path directly and synchronize regular workspace files automatically in both the JavaScript and Rust SDKs.

This is a breaking API change: options objects and manual import or synchronization methods have been removed. Local Lix metadata now always lives in the workspace's `.lix` directory.
