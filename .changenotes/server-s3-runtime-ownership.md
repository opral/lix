---
type: patch
---

Keep server S3 requests and response bodies on the server's long-lived Tokio runtime. Closing a repository's SlateDB runtime no longer invalidates shared HTTP connections used by later repository opens or catalog reads.
