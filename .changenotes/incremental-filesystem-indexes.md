---
type: patch
---

Filesystem path and exact file-ID indexes now advance cached branch views from
committed descriptor deltas. A singleton descriptor commit no longer makes the
next indexed file write scan and reconstruct every visible descriptor;
directory changes update only the affected subtree while immutable index roots
keep concurrent readers on their original generation.
