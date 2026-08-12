---
type: patch
---

The first commit after a bulk import no longer stalls for several hundred milliseconds.

A bulk import could fill the storage engine's write buffers, so the next commit waited on a flush — around 350 ms, against a typical commit of about 20 ms. That commit now takes about 22 ms, in line with every other commit.
