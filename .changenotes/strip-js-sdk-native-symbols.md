---
type: patch
---

Reduced the install size of the `@lix-js/sdk` native binary packages.

Release builds of the native addon now strip the local symbol table, which shrinks the linux-x64 binary from 471 MB to 358 MB without changing its behavior.
