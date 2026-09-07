---
type: patch
---
Use immutable scope certificates and row-primary-key catalogs for bounded packed-state point reads instead of decoding unrelated rows. Preserve file scopes, selected-source identities, and native columnar lookup behavior.
