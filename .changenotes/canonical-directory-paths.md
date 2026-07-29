---
type: minor
---

Directory paths now use the same canonical syntax as file paths.

Non-root paths must not end with `/`; the typed file or directory surface
determines the entity kind. Applications must remove trailing slashes from
directory path values.
