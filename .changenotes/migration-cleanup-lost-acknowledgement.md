---
type: patch
---
Report an unknown write outcome when a repository owner disappears before acknowledging migration cleanup. Require every worker operation to declare its lost-acknowledgement policy, preventing cleanup results from being presented as safely retryable.
