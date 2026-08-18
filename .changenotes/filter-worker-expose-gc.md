---
type: patch
---

Fixed Node.js worker startup when the host process uses worker-incompatible runtime flags such as `--expose-gc`.

The JavaScript SDK no longer forwards worker-incompatible runtime flags while preserving host security restrictions.
