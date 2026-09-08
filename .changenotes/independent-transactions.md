---
type: patch
---

Keep ordinary reads and live queries available while an explicit transaction is open.

Transactions now use an independent context on the originating handle's branch and account. Transaction reads see staged writes, while ordinary reads and observers see committed data. Commit publishes the changes; rollback leaves observers unaffected. Each originating handle still allows one explicit transaction at a time and must finish it before closing.
