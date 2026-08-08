# Immutable W0↔fd2 binding manifest

This directory is the complete TEST/REPORT-only successor package. Every
payload file below except `SHA256SUMS` itself is covered by `SHA256SUMS`; no
production path, Cargo manifest, adapter, or runtime artifact is included.

```text
BINDING.md
MANIFEST.md
README.md
SHA256SUMS
verify_binding.py
```

Anchors:

```text
w0=6a91df3f88177e9b6d53d20d5ba6554df8fd6b9a
fd2=fd2be256d763f17e9f127d4c984e36fba191cb82
fd2_report_package=e2503fd1d43b95d3ebfd133b9868a4be0647ee3d
```

The verifier reads the W0 source/report object with `git show` and checks its
exact tree/blob identities, so this package does not duplicate or silently
rebase the W0 oracle.
