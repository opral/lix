---
type: patch
---

Reduced idle object-store traffic by batching completed SlateDB compactions for
five seconds before publishing them to the manifest.
