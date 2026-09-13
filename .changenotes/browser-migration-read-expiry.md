---
type: patch
---

Fixed full-replica migration inspections failing when OPFS reads expire during migration heartbeat or candidate writes. Frozen source reads now resume bounded point batches and scan pages while checking the exact migration claim and source revision. Local edits and retained source banks remain protected.
