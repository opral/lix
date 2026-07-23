---
type: minor
---

Native binary CAS chunks now use zstd level 1 when compression saves at least
128 bytes and 12.5%, with a raw fallback for small or high-entropy content.
Browser/WASM writers keep raw chunks to avoid a slower, lower-ratio encoder.

All runtimes can decode both codecs. Chunk hashes and deduplication remain
based on the uncompressed bytes.
