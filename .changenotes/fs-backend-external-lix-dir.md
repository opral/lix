---
type: minor
---

Added a `lixDir` option to `FsBackend` for storing lix state outside the workspace.

By default, state lives in `<workspace>/.lix`. Passing `lixDir` keeps repository metadata in an external `.lix` directory and writes no `.lix` directory into the workspace. Pointing `lixDir` at a temporary directory gives ephemeral filesystem sync: workspace files are imported and watched without persisting lix state.
