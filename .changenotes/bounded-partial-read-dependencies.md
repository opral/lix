---
type: patch
target: lix
---
Batch partial-replica dependency discovery and verified content transfer with shared count and byte limits. Plan exact file-content dependencies before transfer, prepare returned-row catalogs together, and retain small validated read closures in bounded memory with durable staging as the fallback. Allow independent resident worker reads to proceed while another operation waits on the network. Preserve old physical repository migration, owner and publication fences, and fail-closed content validation. Local paired browser measurements show faster cold file reads and warm peer updates; warm collaboration still exceeds the 100 ms target.
