---
type: minor
---

Remote browser clients can now persist private client state through Lix.

Pass `storage: new LocalStorage()` to `openLix()` to restore `lix.clientState`
JSON values and the client's active branch without uploading either to the
remote workspace. The dedicated `@lix-js/sdk/remote` entrypoint has been
removed; remote clients use the package root. SQL can read the branch pinned to
its current session with `lix_active_branch_id()`.
