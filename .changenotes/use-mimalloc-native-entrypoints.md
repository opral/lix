---
type: patch
---

Improved native Lix write performance by using mimalloc, an allocator better
suited to its allocation-heavy SQL and storage pipeline.

Bulk tracked updates, inserts, and deletes now spend less CPU time in allocator
bookkeeping. WebAssembly builds and Rust applications embedding the Lix engine
continue to use their host-selected allocator.
