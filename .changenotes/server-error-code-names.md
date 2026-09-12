---
type: minor
---

Normalized reference-server error codes and documented the error catalog.

Server lifecycle errors now use `LIX_ERROR_MIGRATING`, `LIX_ERROR_MIGRATION_FAILED`, `LIX_ERROR_RECOVERING`, `LIX_ERROR_SHUTTING_DOWN`, `LIX_ERROR_CAPACITY`, `LIX_ERROR_CACHE_CLEANUP`, and `LIX_ERROR_TIMEOUT`, removing the redundant second `LIX`. Unsupported storage formats now consistently use `LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT`. This changes the wire codes without legacy aliases; update clients that match the previous spellings together with the server. HTTP statuses and retry semantics are unchanged.
