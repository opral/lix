---
type: patch
---

Improved SQL read performance for table-free queries and fixed Lix system surfaces.

These reads now use immutable system metadata without scanning registered schemas. Runtime schema registrations are rejected only when their generated base, `_by_branch`, or `_history` table names would collide with a fixed system SQL surface.
