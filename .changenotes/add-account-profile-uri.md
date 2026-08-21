---
type: minor
---

Added optional public profile URIs to repository accounts.

Applications can now associate an account with a machine-readable public profile while keeping authentication and authorization separate from presentation metadata.

Existing repositories durably add the nullable account column through the v72 to v73 repository migration.
