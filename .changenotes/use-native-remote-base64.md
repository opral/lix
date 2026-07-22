---
type: patch
---

Remote Lix clients now use native typed-array Base64 conversion when the runtime supports it.

Large blob uploads and observation results avoid the previous byte-by-byte JavaScript conversion cost while retaining the existing compatibility fallback.
