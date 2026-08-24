---
type: minor
---

Added `information_schema.lix_surfaces` for discovering whether each public SQL surface is a relation, table function, command sink, or scalar function, including whether a relation is a base or view.

The catalog also reports read and write capabilities, includes `lix_diff` and `lix_restore`, and classifies composed file, directory, branch, and change relations as views.
