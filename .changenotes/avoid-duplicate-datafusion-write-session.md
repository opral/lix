---
type: patch
---

Improved DataFusion-backed SQL write performance by validating regular fallback writes in their authoritative execution session instead of preparing the same providers twice.
