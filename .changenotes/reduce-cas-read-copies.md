---
type: patch
---

Large file reads now assemble CAS chunks directly from storage-owned bytes.

This removes intermediate copies while preserving the existing storage format and file API.
