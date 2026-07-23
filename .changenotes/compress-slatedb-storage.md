---
type: patch
---

Reduced SlateDB storage and network usage with LZ4 compression.

New SlateDB writes use fast block compression while existing uncompressed databases remain readable without migration.
