---
type: patch
---

Restrict the crates.io release surface to `lix` and the three independently
versioned `lix-storage-*` crates. The CLI, JavaScript binding implementation,
plugins, vendored dependencies, tests, and tooling are private workspace crates,
and CI now rejects accidental additions to the public Cargo surface.
