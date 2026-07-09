---
type: major
---

The filesystem backend replaced its internal state store; existing v0.7 filesystem workspace state is not migrated.

When old metadata is present in `.lix/.internal` and no new store exists, Lix clears `.lix/.internal` and initializes fresh state from the workspace files. Workspace files are untouched; recorded lix history for that workspace is not carried over.
