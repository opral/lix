---
type: minor
---
Return `from_content` and `to_content` from `lix_diff('lix_file', ...)` and `lix_history('lix_file', ...)`. File metadata filters are applied before reading the selected historical bytes, including on partial replicas.

Remove the public diff and history `row_count` column. Count the relation being displayed explicitly with `COUNT(*)`, naming the result `changed_files`, `changed_paragraphs`, or another description of that relation.
