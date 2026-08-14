---
type: patch
---

Consolidated the five internal revision/epoch singletons into one storage space.

Lix tracked "something changed here" with five separate singleton keys, each in
its own storage space scattered across the physical keyspace. They now share one
space and five adjacent keys, so a repository holds four fewer storage spaces,
transaction open reads its catalog revision and tracked-mutation fence with a
single batched lookup, and a commit writes its revisions into one contiguous
region. Repositories written by earlier versions are not readable by this
version.
