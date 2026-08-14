---
type: patch
---

Every write is smaller: Lix no longer keeps a per-publication garbage-collection ledger.

Each commit used to publish a ~439 byte bookkeeping row recording which commit its branch head had just superseded, plus a control row to order those entries — two extra keys in two extra storage planes on every single write, in a plane that only ever grew. Collection now derives the same facts when it runs, from the commit records and physical state a repository already keeps, so those two planes are gone entirely. A repository with 5,000 commits stops carrying 2.2 MB of pure bookkeeping, and superseded branch references and serving caches are now released by the write that supersedes them instead of waiting for a sweep.
