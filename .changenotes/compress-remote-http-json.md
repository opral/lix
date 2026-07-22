---
type: patch
---

Remote Lix clients now gzip large compressible JSON uploads, and protocol servers gzip large finite JSON responses when requested.

A bounded sample avoids spending time compressing small or incompressible uploads. Servers enforce request limits after decompression and leave live SSE streams unbuffered.
