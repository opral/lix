---
type: patch
---

Add authority-fenced replica replacement for inaccessible browser storage. Retirement is durable, account-scoped and idempotent; retired replicas cannot publish through reconnected sessions. Server protocol 12 rejects older clients that cannot participate in replacement. Retired local work remains available for explicit recovery.
