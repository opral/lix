---
type: minor
---

Improved JSON plugin lossless roundtrips, SQL row creation, and scalar edit performance.

JSON objects now preserve duplicate member names, distinguished by an
`occurrence` primary-key column that defaults to zero. Rows provide defaults
for root identities, top-level parents, and ordering, and nested containers can
use caller-supplied identities for SQL creation and renaming. Deeply nested
documents no longer overflow the parser or renderer stack, and numeric changes
remain content changes even when their native numeric values round identically.

Scalar SQL edits locate their rows through a paged identity index and read only
the affected values instead of loading the entire file or scanning every scalar
for each changed row.
