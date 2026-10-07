---
type: patch
target: lix
---
Bound the reference server's retained SlateDB cache file handles across its configured live repository capacity, reserving process descriptor headroom for active reads and runtime work. Closing cached handles preserves cached object bytes and authoritative repository data. Reject invalid zero-handle storage cache configurations before runtime startup.
