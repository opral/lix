---
type: minor
---

Partial replicas adopt remote updates without first fetching data for every previously executed query.

Queries and observers fetch their own missing inputs before returning coherent results. A previously cached query may therefore need a connection after a remote update. Applications can mount their workspace after opening and handle pending queries within each view; missing data is never returned as an empty result.
