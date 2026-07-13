---
type: patch
---
Added deterministic in-memory snapshot import and export to the Workerd JavaScript SDK entry point.

Cloudflare Workers and other Workerd hosts can persist the complete physical Lix state outside an isolate and reopen it without changing branch, commit, or revision identities.
