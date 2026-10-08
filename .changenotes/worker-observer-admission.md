---
type: patch
target: lix
---
Let transient browser view transitions wait for a bounded observer admission slot instead of failing immediately. Use the shared worker scheduler with separate waiting budgets so SQL operations retain capacity. Cancel obsolete registrations across worker startup, owner recovery, and session teardown without leaking late observers.
